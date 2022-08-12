import streamlit as st

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

import plotly.graph_objects as go
from plotly.subplots import make_subplots

# for file IO
import mysql.connector

#conn = mysql.connector.connect(host='13.209.185.189', port = 3306,
#                               user='lgo_marketing_read',
#                               password = 'b56e5a4a5e814b2b9e362706c918fb04',
#                               db = 'laundrygo_prod')

#@st.experimental_memo(ttl=600)
#def run_query(query):
#    cur = conn.cursor(dictionary=True)
#    cur.execute(query)
#    return pd.DataFrame(cur.fetchall())


file_path = './Data/'

# 전일자 기준 - 2022/08/10 기준으로 
prev_date = (date.today() - timedelta(days=1)).isoformat()
prev2_date = (date.today() - timedelta(days=2)).isoformat()



##############################################################
####                                                     #####
####                     필요한 데이터                      #####
####                                                     #####
##############################################################


## 1. user 데이터
user_sql = """
    select date(created_at) as 'created_at' 
        , count(id) as 'user_cnt'
    from user
    where account_type = 'LAUNDRYGO'
        and date(created_at) between date_sub(date(now()), interval 28 day)
                                and date_sub(date(now()), interval 1 day)
    group by date(created_at)
    """
#user = run_query(user_sql)  # 최근 28일치 회원가입자 데이터
user = pd.read_csv(file_path + 'user_0811.csv')
user['created_at'] = pd.to_datetime(user['created_at'].str[:10])
tot_user = user.shape[0]
user = user.groupby(['created_at']).id.count().reset_index(name='user_cnt')


# 1-1. 전체 회원 수 - 탈퇴 회원 카운트 x
tot_user_sql = """
    select count(id) as '전체회원'
    from user
    where account_type = 'LAUNDRYGO'
        and deleted = false
        and date(created_at) <= date_sub(date(now()), interval 1 day)
    """
#tot_user = run_query(tot_user_sql)['전체회원'][0]


## 2. 요금제 가입 & 해지건
plan = """
select created_date as '일자'
    , `요금제 가입건`, `요금제 해지건`
from (select created_date
            , count(1) as '요금제 가입건'
        from (
            select *
            from (
                select user_id, status, date(created_at) as created_date
                from subscription
                where status = 0
                    and date(created_at) between date_sub(date(curdate()), INTERVAL 28 day) and date_sub(date(curdate()), INTERVAL 1 day)
                ) as new_sub
            inner join (select id, account_type from user where account_type = 'LAUNDRYGO') as user1
            on new_sub.user_id = user1.id
            ) as new_sub1
        group by created_date
    ) as new_sub2
inner join (
        select terminated_date
            , count(1) as '요금제 해지건'
        from (
            select *
            from (
                select user_id, status, date(terminated_at) as terminated_date
                from subscription
                where status = 3
                    and date(terminated_at) between date_sub(date(curdate()), INTERVAL 28 day) and date_sub(date(curdate()), INTERVAL 1 day)
                ) as old_sub
            inner join (select id, account_type from user where account_type = 'LAUNDRYGO') as user2
            on old_sub.user_id = user2.id
            ) as old_sub1
        group by terminated_date
    ) as old_sub2

on new_sub2.created_date = old_sub2.terminated_date
"""
#요금제 = run_query(plan)
요금제 = pd.read_csv(file_path + '요금제 가입, 해지_0811.csv')


## 3. 월정액 가입 & 해지건
monthly_plan = """
    select created_date as '일자'
        , `월정액 가입건`, `월정액 해지건`
    from (select created_date
                , count(1) as '월정액 가입건'
            from (
                select *
                from (
                    select user_id, status, date(created_at) as created_date
                    from subscription
                    where status = 0 and laundry_plan_type = 0
                        and date(created_at) between date_sub(date(curdate()), INTERVAL 28 day) and date_sub(date(curdate()), INTERVAL 1 day)
                    ) as new_sub
                inner join (select id, account_type from user where account_type = 'LAUNDRYGO') as user1
                on new_sub.user_id = user1.id
                ) as new_sub1
            group by created_date
        ) as new_sub2
    inner join (
            select terminated_date
                , count(1) as '월정액 해지건'
            from (
                select *
                from (
                    select user_id, status, date(terminated_at) as terminated_date
                    from subscription
                    where status = 3 and laundry_plan_type = 0
                        and date(terminated_at) between date_sub(date(curdate()), INTERVAL 28 day) and date_sub(date(curdate()), INTERVAL 1 day)
                    ) as old_sub
                inner join (select id, account_type from user where account_type = 'LAUNDRYGO') as user2
                on old_sub.user_id = user2.id
                ) as old_sub1
            group by terminated_date
        ) as old_sub2

    on new_sub2.created_date = old_sub2.terminated_date
"""
#월정액 = run_query(monthly_plan)
월정액 = pd.read_csv(file_path + '월정액 가입, 해지_0811.csv')


## 4. 일별 매출액
rev_sql = """
    select
        created_date as '일자'
        , paid_price - cancel_price as '매출액'
    from (select
            sum(case payment_type when 0 then paid_price else 0 end) as paid_price,
            sum(case payment_type when 0 then 0 else paid_price end) as cancel_price,
            date(created_at) as created_date
        from subscription_payment
        where succeeded = 1
            and date(created_at) between date_sub(date(now()), interval 28 day)
                                    and date_sub(date(now()), interval 1 day)
        group by date(created_at)) a
    """
#rev_dat = run_query(rev_sql)
rev_dat = pd.read_csv(file_path + '일별 매출액_0811.csv')


## 5. 수거신청건
wash_dat = pd.read_csv(file_path + '수거신청건_0811.csv').query("`기준 일자` <= @prev_date").sort_values("기준 일자").reset_index(drop=True)


## 6. 커머스, 수선, 프리미엄 매출
rev_dat2 = pd.read_csv(file_path + '커머스, 프리미엄, 수선 매출_0811.csv')
rev_dat2[['커머스', '수선', '프리미엄']] = round(rev_dat2[['커머스', '수선', '프리미엄']] / 1000000, 1)



## 7. 투입 시간(Man Hour)
근무시간 = pd.read_csv(file_path + 'processed_시프티_0804.csv')
근무시간1 = 근무시간.loc[~근무시간.지점.isin(['EPC사업부문', '용산오피스', '강서오피스', '미화파트(강서)',
                              '관리셀(강서)', '관리셀(성수)', '자재관리셀', '커머스관리(군포)',
                              '강서출고관리셀', '성수출고관리셀', '군포출고관리셀', '군포입고관리셀',
                              '군포운송관리셀', '강서운송관리셀', '성수운송관리셀', '운송기획셀',
                              '시설셀(강서)', '시설관리셀', '품질셀(강서)', '품질셀(성수)', '품질셀(군포)', 
                              '운영개선셀(군포)', '강서운영관리셀', '군포운영관리셀', '성수운영관리셀', '강서운영지원파트', '운영지원셀(군포)',
                              '직영기사_통합', '운행기사(PT)', '배송파트(강서)', '강서야간배송파트', '배송파트(성수)'])].reset_index(drop=True)
근무시간1['날짜'] = 근무시간1['날짜'].astype(str)
barcode = pd.read_csv(file_path + '총 바코드 수량_0811.csv')
팩토리_근무시간 = 근무시간1.groupby(['날짜', '팩토리분류'])['근무시간(h)'].sum().reset_index().query("날짜 >= '2022-07-14'")
팩토리전체근무 = pd.merge(팩토리_근무시간, barcode[['일자', '공장구분', '바코드수량']],
                      left_on=['날짜', '팩토리분류'], right_on=['일자', '공장구분'], how='left')
팩토리전체근무 = 팩토리전체근무.drop(columns={'일자', '공장구분'}).fillna(0)



## 8. 3시 이전 입고 비중
입고비중 = pd.read_csv(file_path + '3시 이전 입고_0811.csv').set_index('일자')



## 9. 인건비, 외주용역비
dat_인건비 = pd.read_csv(file_path + '인건비_0803.csv')
인건비_데이터 = dat_인건비.query("구분 == '인건비'")
외주용역비_데이터 = dat_인건비.query("구분 == '외주용역비'")



## 10. 보상
보상 = pd.read_excel(file_path + "7월_보상데이터.xlsx", skiprows=1).iloc[:-1,]
보상['일자'] = 보상['수거일자'].fillna(pd.to_datetime(보상['등록일자']).dt.date.astype(str))
보상2 = 보상.loc[(보상['일자'] >= rev_dat.일자.min()) & (보상['보상금액'] > 0)].groupby(['일자', '공장구분']).agg({'보상금액':'sum', '고유번호':'count'}).reset_index()
보상2.rename(columns={'고유번호':'보상건수'}, inplace=True)
# 팩토리별 보상금액/보상건수/바코드처리량
dat_보상 = pd.merge(보상2, barcode, on=['일자', '공장구분'], how='right').fillna(0)   # 특정 팩토리의 보상금액/건수가 없을 수도 있으니까



## 11. 세탁 관련 문의량
voc = pd.read_csv(file_path + '문의현황_0811.csv')  # 일시, 공장구분, 문의량, 바코드수량 - 공장별 바코드 처리량까지 다 들어있음




################################################################
####                                                        ####
####                       functions                        ####
####                                                        ####
################################################################



## a. 선그래프
def draw_line_graph(dat, x, y, color, name=None):
    if name:
        line = go.Scatter(x = dat[x], y = dat[y], name = name,
                          mode = "lines+markers",
                          line = dict(width = 3, color = color),
                          marker = dict(color = color, size=10), showlegend = True)
    else:
        line = go.Scatter(x = dat[x], y = dat[y], mode = "lines+markers",
                          line = dict(width = 3, color = color),
                          marker = dict(color = color, size=10))
    return line

# a1. 선그래프 for stacked group
def draw_line_graph2(dat, x, y, color, name):
    line = go.Scatter(x = dat[x], y = dat[y], name = name,
                      mode = "lines", #"lines+markers"
                      line = dict(width = 0.5, color = color),
                      #marker = dict(color = color, size=10),
                      showlegend = True, stackgroup='one')
    return line



## b. 선그래프 레이아웃
def linegraph_layout():
    layout = go.Layout(legend = dict(x=0.7, y=0.1, font = {'size': 17},
                                     bordercolor = 'darkgray', borderwidth=1),
                        plot_bgcolor='rgb(255, 255, 255)', paper_bgcolor='rgb(255, 255, 255)',
                        xaxis = {'showline' : True, 'showgrid' : False,
                                'zeroline' : True, 'linecolor':'black', 'linewidth':1.5,
                                'showticklabels' : True, 'ticks' : 'outside', 'tickfont': {'size':17}},
                        yaxis = {'showline' : True, 'showgrid' : True,
                                'gridcolor' : 'lightgray', 'gridwidth': 1.2,
                                'tickfont': {'size':18}})
    return layout



## c. 보통 많이 쓰는 plotly chart figure size
def default_figsize():
    size = go.Layout(autosize=False, width=500, height=450,
                     margin = go.layout.Margin(l=50, r=50, b=10, t=10, pad = 2))
    return size


### (1) 가입자 지표 - Active User

def get_gauge_chart(dau, prev_dau, least_dau, dau_goal):
    data = go.Indicator(mode = "gauge+number+delta", domain = {'x':[0.1, 0.9], 'y':[0.1, 0.9]},
                        value = dau,
                        number = {'font':{'size':60, 'color':'grey'},
                                 'suffix':'명', 'valueformat':','},
                        delta = {'reference':prev_dau, 'relative': True, 'valueformat':'.1%',
                                 'increasing':{'color':'#0dc189'}, 'decreasing':{'color':'salmon'}},
                        gauge = {'axis':{'range':[None, dau_goal],
                                         'tickfont':{'color':'gray', 'size':20},
                                         'tickformat':',', 'tickcolor':'gray'},
                                 'steps':[{'range':[0, prev_dau], 'color':'#e9e9e7'}],
                                 'bar':{'color':'#0dc189', 'thickness':0.5},
                                 'threshold':{'line':{'color':"salmon", 'width':5},
                                              'thickness':0.8, 'value':least_dau+2.5}})
    fig = go.Figure(data=data)
    fig.update_layout(autosize=False, width=600, height=330,
                      margin = go.layout.Margin(l=30, r=30, b=10, t=10, pad = 2))
    
    return fig


### (2) 가입자 지표 - 회원가입
# 리턴값 2개 : 일 회원가입자 수, 회원가입자 28일치 추세
def get_regs_num(dat, today, tot_user):
    # new_regs   :   당일 신규 가입자
    # prev_regs  :   전주(일주일전) 신규 가입자
    # tot_user   :   전체 회원 수(탈퇴 회원 수 집계 x)
    prev_7day = (pd.to_datetime(today) - timedelta(days=7)).isoformat()[:10]
    prev_28day = (pd.to_datetime(today) - timedelta(days=28)).isoformat()[:10]
    
    new_regs = dat.query("created_at == @today")['user_cnt'].values[0]
    prev_regs = dat.query("created_at == @prev_7day")['user_cnt'].values[0]
    
    data = go.Indicator(mode="number+delta",
                        domain={'x':[0, 1], 'y':[0, 1]},
                        value = new_regs,
                        number = {'font':{'color':'gray', 'size':45},
                                  'suffix':'명', 'valueformat':','},
                        delta = {'reference':prev_regs, 'relative': True,
                                 'increasing':{'color':'#0dc189'}, 'decreasing':{'color':'salmon'},
                                 'valueformat':'.1%', 'position':'right'})
    layout1 = go.Layout(autosize = False, width = 500, height = 110,
                        margin = go.layout.Margin(l=50, r=50, b=0, t=0, pad=2))
    fig1 = go.Figure(data=data, layout=layout1)
    
    # 전체 회원 수 정보
    fig1.update_layout(title_text = f"전체 회원 수: {'{:,}'.format(tot_user)}명",
                       title_x = 0.5, title_y = 0.02, 
                       title_font = {'size':24, 'color':'silver'})
    
    # 회원가입자수 선그래프
    fig2 = go.Figure(data = draw_line_graph(dat= dat.query("created_at >= @prev_28day and created_at <= @today"),
                                            x='created_at', y='user_cnt', color='gray'),
                     layout = go.Layout(autosize=False, width=500, height=350,
                                        margin = go.layout.Margin(l=50, r=50, b=10, t=10, pad = 2)))
    fig2.update_layout(linegraph_layout())
    
    return fig1, fig2



### (3) 가입자 지표 - 요금제, 월정액 가입
# 리턴값 2개 : 순증 값, 선 그래프
def get_reg_incs(dat, type, today):
    prev_date = (pd.to_datetime(today) - timedelta(days=7)).isoformat()[:10]
       
    ## 순증 몇 퍼센트
    today_inc = int(dat.query("일자 == @today")[f"{type} 가입건"]) - int(dat.query("일자 == @today")[f"{type} 해지건"])
    yes_inc = int(dat.query("일자 == @prev_date")[f"{type} 가입건"]) - int(dat.query("일자 == @prev_date")[f"{type} 해지건"])

    inc_num = go.Indicator(mode="number+delta",
                            domain={'x':[0, 1], 'y':[0, 1]},
                            value = today_inc,
                            number = {'font':{'color':'gray', 'size':45},
                                    'prefix':'순증 ', 'suffix':'명', 'valueformat':','},
                            delta = {'reference':yes_inc, 'relative': True,
                                     'increasing':{'color':'#0dc189', 'symbol':'+'},
                                     'decreasing':{'color':'salmon', 'symbol':''},
                                     'valueformat':'.1%', 'position':'right'})
    layout1 = go.Layout(autosize = False, width = 500, height = 50,
                        margin = go.layout.Margin(l=50, r=50, b=0, t=0, pad=2))
    fig1 = go.Figure(data=inc_num, layout=layout1)
       
    
    ## 가입, 해지건 선그래프
    fig2 = go.Figure(data = draw_line_graph(dat, x='일자', y=f'{type} 가입건',
                                            name = f'{type} 가입', color='#0dc189'),
                     layout = go.Layout(autosize=False, width=500, height=450,
                                        margin = go.layout.Margin(l=75, r=75, b=10, t=10, pad = 2)))
    fig2.add_trace(draw_line_graph(dat, x='일자', y=f'{type} 해지건',
                                   name = f'{type} 해지', color='silver'))
    fig2.update_layout(linegraph_layout())
    
    return fig1, fig2





################################################################
####                                                        ####
####                      page design                       ####
####                                                        ####
################################################################


## page layout

st.set_page_config(layout = "wide")
st.markdown("<h1 style='text-align: center;'>LaundryGo KPI Dashboard</h1>", unsafe_allow_html=True)
st.markdown(f"<h5 style='text-align: center;'>* {prev_date} 기준</h5></br>", unsafe_allow_html=True)




###########             [[ 가입자 지표 ]]            ############


st.header("[ 가입자 지표 ]")
row1_1, row1_2 = st.columns((1, 1))
_, row2_1, row2_2, row2_3 = st.columns((1, 2, 1, 4))
row3_1, row3_2 = st.columns((1, 1))
row4_1, row4_2 = st.columns((1, 1))
row5_1, row5_2 = st.columns((1, 1))


## (1) dau, mau
with row1_1:
    st.markdown("<h3 style='text-align: center;'>Active User</h3>", unsafe_allow_html=True)
with row2_2:
    AU_option = st.selectbox('', ('Daily', 'Monthly'))
with row2_1:
    au_type = "Daily Active User" if AU_option == 'Daily' else 'Monthly Active User'
    st.markdown(f"<h4 style='text-align:center;'>{au_type}</h4>", unsafe_allow_html=True)

with row3_1:
    prev_dau = 8700; daily_au = 8573; dau_goal = 10000; least_dau = 7000
    prev_mau = 88500; monthly_au = 85899; mau_goal = 100000; least_mau = 80000
       
    if AU_option == 'Daily':
        au_chart = get_gauge_chart(daily_au, prev_dau, least_dau, dau_goal) 
    elif AU_option == 'Monthly':
        au_chart = get_gauge_chart(monthly_au, prev_mau, least_mau, mau_goal)
    
    st.plotly_chart(au_chart, use_container_width=True)



## (2) 회원가입
with row1_2:
    st.markdown("<h3 style='text-align: center;'>일 회원가입</h3>", unsafe_allow_html=True)

new_reg_dash, new_reg_line = get_regs_num(user, prev_date, tot_user)
with row2_3:
    st.plotly_chart(new_reg_dash, use_container_width=True)
with row3_2 :
    st.plotly_chart(new_reg_line, use_container_width=True)
    

## (3) 요금제 가입 / 월정액 가입

# 요금제
요금제_순증, 요금제_fig = get_reg_incs(요금제, '요금제', prev_date)
with row4_1:
    st.markdown("<h3 style='text-align: center;'>요금제 가입</h3>", unsafe_allow_html=True)
    st.plotly_chart(요금제_순증, use_container_width=True)
with row5_1:
    요금제_fig.update_yaxes(range=[0, 450])     # 그래프 y축 범위 조절
    st.plotly_chart(요금제_fig, use_container_width=True)

# 월정액
월정액_순증, 월정액_fig = get_reg_incs(월정액, '월정액', prev_date)
with row4_2:
    st.markdown("<h3 style='text-align: center;'>월정액 가입</h3>", unsafe_allow_html=True)
    st.plotly_chart(월정액_순증, use_container_width=True)
with row5_2:
    월정액_fig.update_yaxes(range=[0, 150])     # 그래프 y축 범위 조절
    st.plotly_chart(월정액_fig, use_container_width=True)

        

        
###########              [[ 수익 지표 ]]             ############

st.header("[ 수익 지표 ]")

row6_1, row6_2 = st.columns((1, 1))
row7_1, row7_2 = st.columns((1, 1))

### (4) 매출
with row6_1:
    st.markdown("<h3 style='text-align: center;'>일 매출</h3>", unsafe_allow_html=True)

    rev_fig = go.Figure(layout = default_figsize())
    # 실제 매출액
    rev_fig.add_trace(go.Bar(x=rev_dat['일자'], y=round(rev_dat['매출액']/10000000, 2), marker_color = '#0dc189', name='실제 매출액'))
    # 예상 매출액
    rev_fig.add_trace(go.Scatter(x=rev_dat['일자'],
                                 y=[8.5503319, 7.6102515, 7.1456286, 6.7880678, 6.798699983, 6.73659038, 6.900811427,
                                    7.031037041, 7.194363065, 7.04793532, 6.664503514, 6.470043818, 6.39374389, 6.432223682,
                                    6.628039118, 6.620248074, 6.523512445, 6.36104181, 6.223326945, 6.178104782, 6.225735289,
                                    6.396432606, 6.40105519, 6.411583775, 6.306665857, 6.230686639, 7.7094624, 7.2644242],
                                 name = '예상 매출액', mode = "lines+markers",
                                 line = dict(width = 3, color = 'darkred'),
                                 marker = dict(color = 'darkred', size=5), showlegend = True))
    # 목표 매출액
    
    rev_fig.update_layout(yaxis_title = '매출액(천만원)')
    
    st.plotly_chart(rev_fig, use_container_width=True)



### (5) 수거신청

with row6_2:
    st.markdown("<h3 style='text-align: center;'>수거신청건</h3>", unsafe_allow_html=True)

    wash_dat['총 매출'] = rev_dat['매출액']
    wash_dat['런드렛당 단가'] = round(wash_dat['총 매출'] / wash_dat['수거신청수'])/1000
    
    # 실제 수거신청건수
    wash_fig = go.Figure(go.Bar(x=wash_dat['기준 일자'], y=wash_dat['수거신청수'], marker_color = '#0dc189', name = '실제량'))
    # 예상 수거신청건수
    wash_fig.add_trace(go.Scatter(x=wash_dat['기준 일자'],
                                  y=[2174, 2451, 2826, 2953, 2889, 2605, 2484,
                                     2215, 2337, 2685, 2617, 2712, 2371, 2194,
                                     1987, 2198, 2626, 2546, 2415, 2158, 2187,
                                     2143, 2187, 2611, 2474, 2491, 2189, 2073], # 마케팅실 - 하반기 예상 물량 엑셀 파일 참고
                                  name = '예상 물량', mode = "lines+markers",
                                  line = dict(width = 3, color = 'darkred'),
                                  marker = dict(color = 'darkred', size=5), showlegend = True))
    
    wash_fig.update_layout(yaxis_title = '수거신청건수(건)',
                           #yaxis = dict(title = '수거신청건수(건)', font = dict(size = 15)),
                           autosize=False, width=500, height=450,
                           margin = go.layout.Margin(l=50, r=50, b=10, t=10, pad = 2))
    
    st.plotly_chart(wash_fig, use_container_width=True)




### (6) 커머스, 프리미엄, 수선 매출
with row7_1:
    st.markdown("<h3 style='text-align: center;'>커머스, 프리미엄, 수선 매출</h3>", unsafe_allow_html=True)
    #st.markdown("<h6 style='text-align: center;'>※ 누적 형태로 그려진 그래프</h6>", unsafe_allow_html=True)
    rev_fig2 = go.Figure(layout = go.Layout(autosize=False, width=500, height=450,
                                            margin = go.layout.Margin(l=50, r=0, b=5, t=5, pad = 2)))
    
    type_lst = ['프리미엄', '수선', '커머스']
    col_lst = ['rgb(13, 100, 86)', '#0dc189', 'rgb(110, 215, 163)']
    for service, col in zip(type_lst, col_lst):
        rev_fig2.add_trace(draw_line_graph2(dat=rev_dat2, x='created_date', y=service,
                                            name = service, color = col))
    
    rev_fig2.update_layout(linegraph_layout())
    rev_fig2.update_layout(go.Layout(legend = dict(x = 1, y=0.5, font = {'size':15}, borderwidth = 0),
                                     yaxis = dict(title = '매출액(백만원)')))
    
    st.plotly_chart(rev_fig2, use_container_width=True)



    
    
###########              [[ 생산성 지표 ]]             ############


st.header("[ 생산성 지표 ]")
_, row8_1, row8_2, row8_3 = st.columns((1, 2, 1, 4))
row9_1, row9_2 = st.columns((1, 1))


## (6) 총 투입시간 대비 처리량
# 팩토리별에서 더 하위 분류인 셀별로 갈 거 같으면 그건 링크로 연결해서 구글 스프레드시트로..!

with row8_1:
    st.markdown("<h3 style='text-align: center;'>총 투입시간(MH) 대비 처리량</h3>", unsafe_allow_html=True)
with row8_2:
    생산성_보기옵션 = st.selectbox('', ('전체 보기', '팩토리별 보기'))

with row9_1:
    근무_fig = go.Figure(layout = default_figsize())
    근무_fig.update_layout(linegraph_layout())
    근무_fig.update_layout(go.Layout(height = 500))
    
    # 전체 보기
    if 생산성_보기옵션 == '전체 보기':
        # 추가적인 전처리
        총팩토리근무 = 팩토리전체근무.groupby(['날짜'])['근무시간(h)', '바코드수량'].sum().reset_index()
        총팩토리근무['투입시간 대비 처리량'] = round(총팩토리근무['바코드수량'] / 총팩토리근무['근무시간(h)'], 2)
        # line graph
        근무_fig.add_trace(draw_line_graph(dat=총팩토리근무, x='날짜', y='투입시간 대비 처리량', color = 'darkgray'))
    
    # 팩토리별 보기
    elif 생산성_보기옵션 == '팩토리별 보기':
        # 추가적인 전처리
        팩토리전체근무['투입시간 대비 처리량'] = round(팩토리전체근무['바코드수량'] / 팩토리전체근무['근무시간(h)'], 2)
        # line graph
        col_lst = ['rgb(13, 100, 86)', '#0dc189', 'rgb(110, 215, 163)']
        for factory, col in zip(['강서공장', '성수공장', '군포공장'], col_lst):
            dat_tmp = 팩토리전체근무.query("팩토리분류 == @factory").reset_index(drop=True)
            근무_fig.add_trace(draw_line_graph(dat=dat_tmp, x='날짜', y='투입시간 대비 처리량', name = factory, color = col))
        # 범례 위치 위로 올리기
        근무_fig.update_layout(go.Layout(legend = dict(orientation="h", xanchor = "center", x=0.5, y=1.12, borderwidth=0)))
    
    근무_fig.update_layout(go.Layout(xaxis = dict(title = '근무일자'),
                                    yaxis = dict(title = '바코드 처리량(개) / 총 투입시간(h)')))                                                          
    st.plotly_chart(근무_fig, use_container_width=True)
    

## (7) 입고 마감시간/비중
with row8_3:
    st.markdown("<h3 style='text-align: center;'>입고 마감시간/비중</h3>", unsafe_allow_html=True)

with row9_2:
    # 입고마감 시간 - 0810 기준
    입고마감 = pd.DataFrame(["10:50", "9:00", "11:30"],
                          index=['강서공장', '성수공장', '군포공장'], columns=['입고마감']).T
    st.dataframe(입고마감, height = 50)
    
    # 3시 이전 입고 비중
    line = go.Figure(layout=linegraph_layout())
    
    factory_lst = ['강서', '성수', '군포']
    col_lst = ['rgb(13, 100, 86)', '#0dc189', 'rgb(110, 215, 163)']
    for factory, col in zip(factory_lst, col_lst):
        dat_tmp = 입고비중[[factory]].dropna().reset_index()
        dat_tmp['일자'] = pd.to_datetime(dat_tmp['일자']).dt.date
        dat_tmp[factory] = dat_tmp[factory].str[:-1].astype(float)
        line.add_trace(draw_line_graph(dat=dat_tmp, x='일자', y=factory, name = factory+'공장', color = col))
    
    line.update_layout(go.Layout(height=380, legend=dict(x=1.0, y=0.5, borderwidth=0),
                                 xaxis=dict(title='일자'), yaxis=dict(title='3시 이전 입고 비중(%)')))
    
    st.plotly_chart(line, use_container_width=True)
    



############              [[ 비용 지표 ]]             #############                    
    

st.header("[ 비용 지표 ]")
#_, row10_1, row10_2, _, row10_3, row10_4 = st.columns((1, 2, 1, 1, 2, 1))
row10_1, _, row10_3, row10_4 = st.columns((4, 1, 2, 1))
row11_1, row11_2 = st.columns((1, 1))


## (8) 인건비, 외주용역비 - 6월 데이터,,,,
with row10_1:
    st.markdown("<h3 style='text-align: center;'>인건비, 외주용역비(6월)</h3>", unsafe_allow_html=True)
#with row10_2:
#    비용_보기옵션 = st.selectbox('', ('전체 보기', '세부항목 보기'))
비용_보기옵션 = '전체 보기'
with row11_1:
    비용_fig1 = go.Figure(layout = default_figsize())
    
    # 최근 달 총 매출
    tot_6rev = 229.7996634
    barcode6 = [227346, 183084, 102638]   # 강서, 성수, 군포 순
    
    if 비용_보기옵션 == '전체 보기':
        fact_6rev = [round(tot_6rev * float(i)/sum(barcode6)) for i in barcode6]
        fact_6rev.insert(0, tot_6rev)
    
        비용_fig1.update_layout(template = "simple_white", yaxis = dict(title_text = "총 비용(천만원)"))
        
        비용_fig1.add_trace(go.Bar(x=[['전체', '강서', '성수', '군포'], ['매출']*4], y=fact_6rev,
                                  width = 0.6, offset=-0.3, name='매출', marker_color = 'silver',
                                  customdata = np.round(fact_6rev, 2),
                                  hovertemplate='%{x[0]} 매출: %{customdata}천만원'))
        # 최근 달 총 비용 - 인건비, 외주용역비 stacked 형태로
        외주용역비 = [외주용역비_데이터[['6월']].astype(int).sum()[0] / 10000000,
                   외주용역비_데이터.loc[외주용역비_데이터['중분류'] == '강서'][['6월']].astype(int).sum()[0] / 10000000,
                   외주용역비_데이터.loc[외주용역비_데이터['중분류'] == '성수'][['6월']].astype(int).sum()[0] / 10000000,
                   외주용역비_데이터.loc[외주용역비_데이터['중분류'] == '군포'][['6월']].astype(int).sum()[0] / 10000000]
        인건비 = [인건비_데이터[['6월']].astype(int).sum()[0] / 10000000,
                인건비_데이터.loc[인건비_데이터['중분류'] == '강서'][['6월']].astype(int).sum()[0] / 10000000,
                인건비_데이터.loc[인건비_데이터['중분류'] == '성수'][['6월']].astype(int).sum()[0] / 10000000,
                인건비_데이터.loc[인건비_데이터['중분류'] == '군포'][['6월']].astype(int).sum()[0] / 10000000]
        # - 외주용역비
        #customdata=[]
        비용_fig1.add_trace(go.Bar(x=[['전체', '강서', '성수', '군포'], ['비용']*4], y=list(sum(np.array([외주용역비, 인건비]), 0)),
                                  name='외주용역비', marker=dict(color='#0dc189'),
                                  width=0.6, offset=-0.3,
                                  customdata= np.round(외주용역비, 2),
                                  hovertemplate='%{x[0]} 외주용역비: %{customdata}천만원'))
        # - 인건비
        비용_fig1.add_trace(go.Bar(x=[['전체', '강서', '성수', '군포'], ['비용']*4], y=인건비,
                                  name='인건비', marker=dict(color='rgb(13, 100, 86)'),
                                  width = 0.6, offset=-0.3,
                                  customdata = np.round(인건비, 2),
                                  hovertemplate='%{x[0]} 인건비: %{customdata}천만원'))
        # 범례위치 조절
        비용_fig1.update_layout(legend=dict(x=0.8, y=0.97, borderwidth=0.1))
    
    elif 비용_보기옵션 == '세부항목 보기':
        pass
        
    
    st.plotly_chart(비용_fig1, use_container_width = True)


## (9) 유틸리티(수도, 가스, 전기)
with row10_3:
    st.markdown("<h3 style='text-align: center;'>유틸리티(수도, 가스, 전기/6월)</h3>", unsafe_allow_html=True)        
with row10_4:
    비용_보기옵션2 = st.selectbox('', ('절댓값', '물량 대비'))

dat_유틸리티 = pd.DataFrame([[57769770, 34250100, 19096920, 4422750],
                         [74526590, 36396290, 15319480, 22810820],
                         [49842620, 15489860, 19618650, 14734110]],
                        index = ['수도', '가스', '전기'], columns = ['전체', '강서', '성수', '군포']).T / 1000000
    
with row11_2:
    st.write("* 비고 : 강서 상하수도 격월납부(짝수월)")
    
    if 비용_보기옵션2 == '절댓값':
        barchart = [go.Bar(x=['전체', '강서', '성수', '군포'], y=dat_유틸리티['수도'].values,
                           name='수도', marker=dict(color='rgb(110, 215, 163)'),
                           customdata= np.round(dat_유틸리티[['수도']], 2),
                           hovertemplate='%{x} 가스: %{customdata}백만원'),
                    go.Bar(x=['전체', '강서', '성수', '군포'], y=dat_유틸리티['가스'].values,
                           name='수도', marker=dict(color='#0dc189'),
                           customdata= np.round(dat_유틸리티[['가스']], 2),
                           hovertemplate='%{x} 가스: %{customdata}백만원'),
                    go.Bar(x=['전체', '강서', '성수', '군포'], y=dat_유틸리티['전기'].values,
                           name='전기', marker=dict(color='rgb(13, 100, 86)'),
                           customdata = np.round(dat_유틸리티[['전기']], 2),
                           hovertemplate='%{x} 전기: %{customdata}백만원')]
        유틸리티_fig = go.Figure(data=barchart, layout = default_figsize())
        유틸리티_fig.update_layout(yaxis = dict(title_text = "총 비용(백만원)"))
                                 #, legend = dict(x=0.9, y=0.97))
        유틸리티_fig.update_layout(go.Layout(height=420))
        
        
    elif 비용_보기옵션2 == '물량 대비':
        # 6월달 팩토리별 처리 물량
        dat_유틸리티2 = dat_유틸리티.loc[['강서', '성수', '군포']] * 1000000
        dat_유틸리티2['처리량'] = barcode6
        
        dat_유틸리티2['수도'] = dat_유틸리티2['수도'] / dat_유틸리티2['처리량']
        dat_유틸리티2['가스'] = dat_유틸리티2['가스'] / dat_유틸리티2['처리량']
        dat_유틸리티2['전기'] = dat_유틸리티2['전기'] / dat_유틸리티2['처리량']
        
        barchart = [go.Bar(x=['수도', '가스', '전기'], y=dat_유틸리티2.loc['강서'].values, name='강서공장',
                           customdata = np.round(dat_유틸리티2.loc['강서'].values, 2),
                           hovertemplate='[강서] %{x}: %{customdata}원'),
                    go.Bar(x=['수도', '가스', '전기'], y=dat_유틸리티2.loc['성수'].values, name='성수공장',
                           customdata = np.round(dat_유틸리티2.loc['성수'].values, 2),
                           hovertemplate='[성수] %{x}: %{customdata}원'),
                    go.Bar(x=['수도', '가스', '전기'], y=dat_유틸리티2.loc['군포'].values, name='군포공장',
                           customdata = np.round(dat_유틸리티2.loc['군포'].values, 2),
                           hovertemplate='[군포] %{x}: %{customdata}원')]
        
        유틸리티_fig = go.Figure(data=barchart, layout = default_figsize())
        유틸리티_fig.update_layout(barmode='group')
        
    st.plotly_chart(유틸리티_fig, use_container_width = True)
        
    


###########              [[ 품질 지표 ]]             ############


st.header("[ 품질 지표 ]")
st.write("* 비고 : 수거일자 및 세탁처리일이 누락되었을 경우 문의일로 집계")

_, row12_1, row12_2, _, row12_3, row12_4 = st.columns((1, 2, 1, 1, 2, 1))
row13_1, row13_2 = st.columns((1, 1))
_, row14_1, row14_2, _ = st.columns((1, 2, 1, 4))
row15_1, row15_2 = st.columns((1, 1))


## (8) 처리량 대비 보상건수
with row12_1:
    st.markdown("<h3 style='text-align: center;'>처리량 대비 보상건수</h3>", unsafe_allow_html=True)
with row12_2:
    품질_보기옵션 = st.selectbox('', ('전체', '팩토리별'))
  
with row13_1:
    보상건수_fig = go.Figure(layout = default_figsize())
    보상건수_fig.update_layout(linegraph_layout())
    
    # 전체 보기
    if 품질_보기옵션 == '전체':
        # 추가적인 전처리
        보상건수 = dat_보상.groupby(['일자'])['보상건수', '바코드수량'].sum().reset_index()
        보상건수['처리량 대비 보상건수'] = round(보상건수['보상건수'] / 보상건수['바코드수량'], 4) * 100
        # line graph
        보상건수_fig.add_trace(draw_line_graph(dat=보상건수, x='일자', y='처리량 대비 보상건수', color = 'darkgray'))
        
        보상건수_fig.update_yaxes(range=[0, 0.15])     # 그래프 y축 범위 조절
    
    # 팩토리별 보기
    elif 품질_보기옵션 == '팩토리별':
        # 추가적인 전처리
        팩토리별_보상건수 = dat_보상.copy()
        팩토리별_보상건수['처리량 대비 보상건수'] = round(팩토리별_보상건수['보상건수'] / 팩토리별_보상건수['바코드수량'], 4) * 100
        # line graph
        col_lst = ['rgb(13, 100, 86)', '#0dc189', 'rgb(110, 215, 163)']
        for factory, col in zip(['강서공장', '성수공장', '군포공장'], col_lst):
            보상건수_fig.add_trace(draw_line_graph(dat=팩토리별_보상건수.query("공장구분 == @factory"),
                                                 x='일자', y='처리량 대비 보상건수', name = factory, color = col))
        # 범례 위치 위로 올리기
        보상건수_fig.update_layout(go.Layout(legend = dict(orientation="h", xanchor = "center", x=0.5, y=1.12, borderwidth=0)))
        보상건수_fig.update_yaxes(range=[0, 0.25])     # 그래프 y축 범위 조절 > 나중에 그 기간중 max 값 + 0.2~3 정도로 로직 바꾸기
    
    
    보상건수_fig.update_layout(go.Layout(xaxis = dict(title = '수거일자'),
                                       yaxis = dict(title = '보상건수/바코드처리량 (%)'),
                                       margin = go.layout.Margin(b=0,t=0, pad=2)))
        
    st.plotly_chart(보상건수_fig, use_container_width=True)
    
    
    

## (9) 매출액 대비 보상금액
with row12_3:
    st.markdown("<h3 style='text-align: center;'>매출액 대비 보상금액</h3>", unsafe_allow_html=True)
with row12_4:
    품질_보기옵션2 = st.selectbox('', ('전체보기', '팩토리별보기'))

with row13_2:
    보상금액_fig = go.Figure(layout = go.Layout(autosize=False, width=500, height=450,
                                              margin = go.layout.Margin(l=50, r=10, b=10, t=0, pad = 2)))
    
    
    if 품질_보기옵션2 == '전체보기':
        # 약간의 전처리 추가 : 전체 매출액 대비 보상금액
        보상금액 = pd.merge(dat_보상.groupby(['일자'])['보상금액'].sum().reset_index(), rev_dat, on='일자')
        보상금액['매출액 대비 보상금액'] = round(보상금액['보상금액'] / 보상금액['매출액'], 4) * 100
        
        # line graph
        tot_line = draw_line_graph(dat=보상금액, x='일자', y='매출액 대비 보상금액', color = 'darkgray')
        보상금액_fig.add_trace(tot_line)
    
    elif 품질_보기옵션2 == '팩토리별보기':
        # 추가적인 전처리
        # 팩토리별 처리량 고려한 매출액 분배
        팩토리별_보상금액 = pd.merge(dat_보상, rev_dat, on='일자')
        팩토리별_보상금액['팩토리별 매출액'] = round(팩토리별_보상금액['매출액'] * 팩토리별_보상금액['바코드수량'] / 팩토리별_보상금액['일별 총 바코드']) 
        팩토리별_보상금액['매출액 대비 보상금액'] = round(팩토리별_보상금액['보상금액'] / 팩토리별_보상금액['팩토리별 매출액'], 4) * 100 
        
        factory_lst = ['강서공장', '성수공장', '군포공장']
        col_lst = ['rgb(13, 100, 86)', '#0dc189', 'rgb(110, 215, 163)']
        for factory, col in zip(factory_lst, col_lst):
            보상금액_fig.add_trace(draw_line_graph(dat=팩토리별_보상금액.query("공장구분 == @factory"), x='일자', y='매출액 대비 보상금액',
                                                 name = factory, color = col))
    
    보상금액_fig.update_layout(linegraph_layout())
    #보상금액_fig.update_yaxes(range=[0, 4])     # 그래프 y축 범위 조절
    보상금액_fig.update_layout(go.Layout(legend = dict(x = 1, y=0.5, font = {'size':15}, borderwidth = 0),
                                       xaxis = dict(title='수거일자'), yaxis = dict(title = '보상금액/매출액(%)')))
        
    st.plotly_chart(보상금액_fig, use_container_width=True)


    
## (10) 세탁 관련 voc 인입률
with row14_1:
    st.markdown("<h3 style='text-align: center;'>세탁 관련 VOC 인입률</h3>", unsafe_allow_html=True)
with row14_2:
    voc_보기옵션 = st.selectbox('', ('전체보기', '팩토리별'))
    
with row15_1:
    # 추가적인 데이터 전처리
    tot_voc_dat = voc.groupby(['일시'])['바코드수량', '문의량'].sum().reset_index()
    tot_voc_dat['인입률'] = round(tot_voc_dat['문의량'] / tot_voc_dat['바코드수량'], 4) * 100
    
    if voc_보기옵션 == '전체보기':
        # chart
        voc_fig = make_subplots(specs = [[{"secondary_y": True}]])
        voc_fig.add_trace(go.Scatter(x=tot_voc_dat['일시'], y=tot_voc_dat['인입률'],
                                      name = '인입률(%)', mode = "lines+markers",
                                      line = dict(width = 3, color = '#0dc189'),
                                      marker = dict(color = '#0dc189', size=10)))
        voc_fig.add_trace(go.Bar(x=tot_voc_dat['일시'], y=tot_voc_dat['바코드수량'], name = '처리량(바코드 기준)',
                                 marker_color='#e9e9e7', opacity=0.4),
                          secondary_y = True)

        voc_fig.update_layout(go.Layout(legend = dict(x=0.73, y=1.25, font = {'size': 15}),
                                        plot_bgcolor='rgb(255, 255, 255)', paper_bgcolor='rgb(255, 255, 255)',
                                        xaxis = {'showline' : True, 'showgrid' : False,
                                                 'zeroline' : True, 'linecolor':'black', 'linewidth':1.5,
                                                 'showticklabels' : True, 'ticks' : 'outside',
                                                 'tickfont': {'size':17}},
                                        yaxis = {'showline' : True, 'showgrid' : True,
                                                 'gridcolor' : 'lightgray', 'gridwidth': 1.2,
                                                 'tickfont': {'size':17}}))
        voc_fig.update_layout(autosize=False, width=500, height=450,
                               margin = go.layout.Margin(l=50, r=50, b=10, t=10, pad = 2))
        voc_fig.update_layout(yaxis_title = 'VOC 인입률(%)', showlegend=True)
        voc_fig.update_yaxes(title_text='처리량(바코드 기준)', secondary_y = True)
    
    elif voc_보기옵션 == '팩토리별':
        # 추가적인 전처리
        voc['인입률'] = round(voc['문의량'] / voc['바코드수량'], 4) * 100

        ## chart
        voc_fig = go.Figure(layout = default_figsize())
        voc_fig.update_layout(linegraph_layout())
        voc_fig.update_layout(go.Layout(height=470))
        # 전체 Line graph
        voc_fig.add_trace(go.Scatter(x=tot_voc_dat['일시'], y=tot_voc_dat['인입률'],
                                     name = '전체', mode = "lines", line = dict(width = 4, color = 'gray', dash='dash')))
        # 팩토리별 Line graph
        col_lst = ['rgb(13, 100, 86)', '#0dc189', 'rgb(110, 215, 163)']
        for factory, col in zip(['강서공장', '성수공장', '군포공장'], col_lst):
            dat_tmp = voc.query("공장구분 == @factory").reset_index(drop=True)
            voc_fig.add_trace(draw_line_graph(dat=dat_tmp, x='일시', y='인입률', name = factory, color = col))
        # 범례 위치 위로 올리기
        voc_fig.update_layout(go.Layout(legend = dict(orientation="h", xanchor = "center", x=0.5, y=1.12, borderwidth=0)))
        # x, y축 이름 붙이기
        voc_fig.update_layout(go.Layout(xaxis = dict(title = '세탁처리일'),
                                        yaxis = dict(title = 'VOC 인입률(%)')))
    
    st.plotly_chart(voc_fig, use_container_width=True)
    

#######################################################


