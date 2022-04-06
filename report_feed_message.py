import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import os
import pandas as pd
import pandahouse
from datetime import date, timedelta

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20220320'
}

def select(q):
    return pandahouse.read_clickhouse(q, connection=connection)

sns.set_style('darkgrid')
sns.set_palette('bright')
sns.set_context('notebook')



def test_report(chat=None):
    chat_id = chat or -1001539201117
    # chat_id = chat or 453565850
    
    bot = telegram.Bot(token=os.environ.get("REPORT_BOT_TOKEN"))
    
    # создание метрик за вчерашний день, также изменение этих по сравнению с неделей назад (WoW)
    df = select('SELECT user_id, action FROM simulator_20220320.feed_actions WHERE toDate(time) = yesterday()')
    df_7 = select('SELECT user_id, action FROM simulator_20220320.feed_actions WHERE toDate(time) = yesterday()-7')

    DAU_feed = df['user_id'].nunique()
    view = len(df[df['action'] == 'view'])
    like = len(df[df['action'] == 'like'])
    CTR = like/view * 100

    DAU_feed_WoW = (DAU_feed - df_7['user_id'].nunique()) / df_7['user_id'].nunique() * 100
    view_WoW = (view - len(df_7[df_7['action'] == 'view'])) / len(df_7[df_7['action'] == 'view']) * 100
    like_WoW = (like - len(df_7[df_7['action'] == 'like'])) / len(df_7[df_7['action'] == 'like']) * 100

    CTR_7 = len(df_7[df_7['action'] == 'like']) / len(df_7[df_7['action'] == 'view']) * 100
    CTR_WoW = (CTR - CTR_7) / CTR_7 * 100

    
    # новые пользователи за вчерашний день и сравнение с неделью назад
    df = select("""
        SELECT COUNT(user_id) FROM (
        SELECT DISTINCT user_id FROM simulator_20220320.feed_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = yesterday())""")

    df_7 = select("""
        SELECT COUNT(user_id) FROM ( 
        SELECT DISTINCT user_id FROM simulator_20220320.feed_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = yesterday()-7)""")

    new_users_feed = int(df.iloc[0, 0])
    new_users_feed_WoW = (new_users_feed - int(df_7.iloc[0, 0])) / int(df_7.iloc[0, 0]) * 100

    # аналогично с новыми постами
    df = select("""
        SELECT COUNT(post_id) FROM (
        SELECT DISTINCT post_id FROM simulator_20220320.feed_actions
        GROUP BY post_id
        HAVING MIN(toDate(time)) = yesterday())""")

    df_7 = select("""
        SELECT COUNT(post_id) FROM ( 
        SELECT DISTINCT post_id FROM simulator_20220320.feed_actions
        GROUP BY post_id
        HAVING MIN(toDate(time)) = yesterday()-7)""")

    new_posts = int(df.iloc[0, 0])
    new_posts_WoW = (new_posts - int(df_7.iloc[0, 0])) / int(df_7.iloc[0, 0]) * 100



    # создание метрик DAU, число сообщений, число сообщений на пользователя + сравнение с предыдущей неделю
    df = select('SELECT user_id FROM simulator_20220320.message_actions WHERE toDate(time) = yesterday()')
    df_7 = select('SELECT user_id FROM simulator_20220320.message_actions WHERE toDate(time) = yesterday()-7')

    DAU_message = df['user_id'].nunique()
    messages = len(df)
    messages_per_user = messages/DAU_message
    DAU_message_WoW = (DAU_message - df_7['user_id'].nunique()) / df_7['user_id'].nunique() * 100
    messages_WoW = (messages - len(df_7)) / len(df_7) * 100
    
    messages_per_user_7 = df_7['user_id'].nunique()/len(df) * 100
    messages_per_user_WoW = (messages_per_user - messages_per_user_7) / messages_per_user_7 * 100

    df = select("""
        SELECT COUNT(user_id) FROM (
        SELECT DISTINCT user_id FROM simulator_20220320.message_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = yesterday())""")
    df_7 = select("""
        SELECT COUNT(user_id) FROM (
        SELECT DISTINCT user_id FROM simulator_20220320.message_actions
        GROUP BY user_id
        HAVING MIN(toDate(time)) = yesterday()-7)""")

    # прирост новых ползователей сервиса сообщений
    new_users_message = int(df.iloc[0, 0])
    new_users_message_WoW = (new_users_message - int(df_7.iloc[0, 0])) / int(df_7.iloc[0, 0]) * 100

    yesterday = (date.today() - timedelta(days=1)).strftime("%d.%m.%Y")

    msg = f"""Данные за вчерашний день {yesterday}
      Лента новостей:
        DAU - {DAU_feed} {'+' if DAU_feed_WoW > 0 else ''}{DAU_feed_WoW:.2f}% WoW,
        Просмотры - {view} {'+' if view_WoW > 0 else ''}{view_WoW:.2f}% WoW,
        Лайки - {like} {'+' if like_WoW > 0 else ''}{like_WoW:.2f}% WoW,
        СTR - {CTR:.2f}% {'+' if CTR_WoW > 0 else ''}{CTR_WoW:.2f}% WoW,
        Новые пользователи - {new_users_feed} {'+' if new_users_feed_WoW > 0 else ''}{new_users_feed_WoW:.2f}% WoW,
        Новые посты - {new_posts} {'+' if new_posts_WoW > 0 else ''}{new_posts_WoW:.2f}% WoW,

      Сервис сообщений:
        DAU - {DAU_message} {'+' if DAU_message_WoW > 0 else ''}{DAU_message_WoW:.2f}% WoW,
        Количество сообщений - {messages} {'+' if messages_WoW > 0 else ''}{messages_WoW:.2f}% WoW,
        Сообщений на одного пользователя - {messages_per_user:.2f} {'+' if messages_per_user_WoW > 0 else ''}{messages_per_user_WoW:.2f}% WoW,
        Новые пользователи - {new_users_message} {'+' if new_users_message_WoW > 0 else ''}{new_users_message_WoW:.2f}% WoW"""

    # print(msg)
    
    # получить метрики ленты за прошедшую неделю
    df = select("""
    SELECT toDate(time) date,
           COUNT(DISTINCT user_id) DAU,
           countIf(user_id, action='view') views,
           countIf(user_id, action='like') likes,
           likes/views * 100 CTR
    FROM simulator_20220320.feed_actions 
    WHERE toDate(time) > yesterday()-7 AND toDate(time) <= yesterday()
    GROUP BY toDate(time)""")

    # получить прирост новых пользователей за прошедшую неделю
    df_new_user = select("""
    SELECT COUNT(user_id) new_users, time FROM (
        SELECT DISTINCT user_id, MIN(toDate(time)) time FROM simulator_20220320.feed_actions
    GROUP BY user_id)
    WHERE time BETWEEN yesterday()-6 AND yesterday()
    GROUP BY time
    ORDER BY time""")
    
    # получить прирост новых постов за прошедшую неделю
    df_new_post = select("""
    SELECT COUNT(post_id) new_posts, time FROM (
        SELECT DISTINCT post_id, MIN(toDate(time)) time FROM simulator_20220320.feed_actions
    GROUP BY post_id)
    WHERE time BETWEEN yesterday()-6 AND yesterday()
    GROUP BY time
    ORDER BY time""")

    df['new_users'] = df_new_user['new_users']
    df['new_posts'] = df_new_post['new_posts']
    df['Дата'] = df['date'].dt.strftime('%d.%m')

    # получить метрики сервиса сообщений за прошедшую неделю
    df_message = select("""
    SELECT toDate(time) date,
          COUNT(DISTINCT user_id) DAU,
          COUNT(user_id) messages,
          messages/DAU MpU
    FROM simulator_20220320.message_actions 
    WHERE toDate(time) > yesterday()-7 AND toDate(time) <= yesterday()
    GROUP BY toDate(time)""")

    # получить прирост новых пользователей сервиса сообщений за прошедшую неделю
    df_new_user_message = select("""
    SELECT COUNT(user_id) new_users, time FROM (
        SELECT DISTINCT user_id, MIN(toDate(time)) time FROM simulator_20220320.message_actions
    GROUP BY user_id)
    WHERE time BETWEEN yesterday()-6 AND yesterday()
    GROUP BY time
    ORDER BY time""")

    df_message['new_users'] = df_new_user_message['new_users']
    df_message['Дата'] = df_message['date'].dt.strftime('%d.%m')

    # получить топ 10 просматривыемых постов за прошедшую неделю
    top_10_post = select("""
    SELECT post_id, COUNT(user_id) views 
    FROM simulator_20220320.feed_actions
    WHERE toDate(time) = yesterday()
    GROUP BY post_id
    ORDER BY COUNT(user_id) DESC
    LIMIT 10""")
    
    file_object = io.BytesIO()
    top_10_post.to_csv(file_object)
    file_object.name = 'Top 10 posts.csv'
    file_object.seek(0)

     
    
    bot.sendMessage(chat_id=chat_id, text=msg)

    
    # линейный график на каждую метрику ленты новостей
    fig, ax = plt.subplots(nrows=6, ncols=1, sharex=True, figsize=(12,12))

    title = f'Лента новостей\nЗначения метрик с {(date.today() - timedelta(days=7)).strftime("%d.%m.%Y")} по {yesterday}'
    plt.suptitle(title)

    sns.lineplot(x=df['Дата'], y=df['DAU'], marker='o', ax=ax[0])

    sns.lineplot(x=df['Дата'], y=df['views'], marker='o', ax=ax[1])
    ax[1].set_ylabel('Просмотры')

    sns.lineplot(x=df['Дата'], y=df['likes'], marker='o', ax=ax[2])
    ax[2].set_ylabel('Лайки')

    sns.lineplot(x=df['Дата'], y=df['CTR'], marker='o', ax=ax[3])
    ax[3].set_ylabel('CTR, %')

    sns.lineplot(x=df['Дата'], y=df['new_users'], marker='o', ax=ax[4])
    ax[4].set_ylabel('Новые\nпользователи')

    sns.lineplot(x=df['Дата'], y=df['new_posts'], marker='o', ax=ax[5])
    ax[5].set_ylabel('Новые\nпосты');

    fig_object = io.BytesIO()
    plt.tight_layout()
    plt.savefig(fig_object)
    fig_object.name = 'report.png'
    fig_object.seek(0)
    plt.close()

    
    bot.sendPhoto(chat_id=chat_id, photo=fig_object)

    
    
    # линейный график на каждую метрику сервиса сообщений
    fig, ax = plt.subplots(nrows=4, ncols=1, sharex=True, figsize=(12,12))

    title = f'Сервис сообщений\nЗначения метрик с {(date.today() - timedelta(days=7)).strftime("%d.%m.%Y")} по {yesterday}'
    plt.suptitle(title)

    sns.lineplot(x=df_message['Дата'], y=df_message['DAU'], marker='o', ax=ax[0])

    sns.lineplot(x=df_message['Дата'], y=df_message['messages'], marker='o', ax=ax[1])
    ax[1].set_ylabel('Количество\nсообщений')
    ax[1].set_ylim([12885, 12910])

    sns.lineplot(x=df_message['Дата'], y=df_message['MpU'], marker='o', ax=ax[2])
    ax[2].set_ylabel('Сообщений\nна одного\nпользователя')

    sns.lineplot(x=df_message['Дата'], y=df_message['new_users'], marker='o', ax=ax[3])
    ax[3].set_ylabel('Новые\nпользователи');

    fig_object = io.BytesIO()
    plt.tight_layout()
    plt.savefig(fig_object)
    fig_object.name = 'report.png'
    fig_object.seek(0)
    plt.close()

    
    bot.sendPhoto(chat_id=chat_id, photo=fig_object)
    
    bot.sendDocument(chat_id=chat_id, document=file_object)
    
    
try:
    test_report()
except Exception as e:
    print(e)