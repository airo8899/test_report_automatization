import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io
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

def select(q):
    return pandahouse.read_clickhouse(q, connection=connection)

def test_report(chat=None):
    chat_id = chat or 453565850
    bot = telegram.Bot(token='5167010511:AAETy3cSIsBkRmmrI-4DmhMTVurzlwfVLi4')

    df = select('SELECT user_id, action FROM simulator_20220320.feed_actions WHERE toDate(time) = yesterday()')
    
    DAU = df['user_id'].nunique()
    view = len(df[df['action'] == 'view'])
    like = len(df[df['action'] == 'like'])
    CTR = round(like/view * 100, 2)
    
    yesterday = (date.today() - timedelta(days=1)).strftime("%d.%m.%Y")
    
    msg = f"""Данные за вчерашний день {yesterday}:
    DAU - {DAU},
    Просмотры - {view},
    Лайки - {like},
    СTR - {CTR}%"""
        
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    
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
