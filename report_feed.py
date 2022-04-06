import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import pandas as pd
import pandahouse
from datetime import date, timedelta

sns.set_style('darkgrid')
sns.set_palette('bright')
sns.set_context('notebook')

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220320'
}


# функция запроса к бд
def select(q):
    return pandahouse.read_clickhouse(q, connection=connection)


def test_report(chat=None):
    chat_id = chat or -1001539201117
    # chat_id = chat or 453565850
    
    bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))
    
    # запрос юзера и типа действия за вчерашний день
    df = select("""SELECT user_id, action 
                    FROM simulator_20220320.feed_actions 
                    WHERE toDate(time) = yesterday()""")
    
    
    DAU = df['user_id'].nunique()
    view = len(df[df['action'] == 'view'])
    like = len(df[df['action'] == 'like'])
    CTR = round(like/view * 100, 2)
    
    # создание строковой даты вида ДД.ММ.ГГГГ
    yesterday = (date.today() - timedelta(days=1)).strftime("%d.%m.%Y")
    
    msg = f"""Данные за вчерашний день {yesterday}:
    DAU - {DAU},
    Просмотры - {view},
    Лайки - {like},
    СTR - {CTR}%"""
        
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    # запрос даты, DAU, число просмотров, лайков, CTR от 8 дней назад до вчерашнего дня
    df = select("""SELECT toDate(time) date,
                       COUNT(DISTINCT user_id) DAU,
                       countIf(user_id, action='view') views,
                       countIf(user_id, action='like') likes,
                       ROUND(likes/views * 100, 2) CTR
                FROM simulator_20220320.feed_actions 
                WHERE toDate(time) > yesterday()-7 AND toDate(time) <= yesterday()
                GROUP BY toDate(time)""")
    
    df.columns = ['date', 'DAU', 'Просмотры', 'Лайки', 'CTR, %']
    df['Дата'] = df['date'].dt.strftime('%d.%m')
    
    # создание графика 1 столбец, 4 строки
    g = sns.PairGrid(data=df,
                 x_vars=['Дата'],
                 y_vars=['DAU', 'Просмотры', 'Лайки', 'CTR, %'],
                 aspect=4)

    title = f'Значения метрик с {(date.today() - timedelta(days=7)).strftime("%d.%m.%Y")} по {yesterday}'
    g.fig.suptitle(title, size=16)

    g.map(sns.lineplot);

    fig_object = io.BytesIO()
    plt.tight_layout()
    plt.savefig(fig_object)
    fig_object.name = 'report.png'
    fig_object.seek(0)
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=fig_object)

try:
    test_report()
except Exception as e:
    print(e)
