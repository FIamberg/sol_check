import streamlit as st
import pandas as pd
import mysql.connector
import datetime

st.set_page_config(layout="wide")

def init_connection():
    return mysql.connector.connect(
        host="185.120.57.125",
        user="admin",
        password="v8S7b$82j51d1",
        database="crypto"
    )

def get_connection():
    if 'conn' not in st.session_state:
        st.session_state.conn = init_connection()
    return st.session_state.conn

@st.cache_data(ttl=5*60)
def fetch_data(_conn, date_from=None, date_to=None):
    query = """
    SELECT 
        *
    FROM solana_parser
    """

    if date_from and date_to:
        query += " WHERE DATE BETWEEN %s AND %s"
        query += " ORDER BY DATE desc"
        df = pd.read_sql(query, _conn, params=[date_from, date_to])
    else:
        query += " ORDER BY DATE desc"
        df = pd.read_sql(query, _conn)
    
    return df

def create_summary_table(df):
    # Создаем DataFrame для покупок (received_currency)
    buys = df[['received_currency', 'wallet_address', 'swapped_value_USD']].rename(columns={
        'received_currency': 'coin',
        'swapped_value_USD': 'volume'
    })
    buys['transaction_type'] = 'buy'

    # Создаем DataFrame для продаж (swapped_currency)
    sells = df[['swapped_currency', 'wallet_address', 'swapped_value_USD']].rename(columns={
        'swapped_currency': 'coin',
        'swapped_value_USD': 'volume'
    })
    sells['transaction_type'] = 'sell'

    # Объединяем покупки и продажи
    combined = pd.concat([buys, sells])

    # Группируем и агрегируем данные
    summary = combined.groupby(['coin', 'transaction_type']).agg({
        'wallet_address': 'nunique',
        'volume': 'sum'
    }).reset_index()

    # Создаем сводную таблицу
    summary_pivot = summary.pivot(index='coin', columns='transaction_type', 
                                  values=['wallet_address', 'volume'])
    
    # Сглаживаем иерархию столбцов
    summary_pivot.columns = [f'{col[1]}_{col[0]}' for col in summary_pivot.columns]
    summary_pivot = summary_pivot.reset_index()
    
    # Переименовываем столбцы
    column_mapping = {
        'buy_wallet_address': 'buy_wallets',
        'sell_wallet_address': 'sell_wallets',
        'buy_volume': 'buy_volume',
        'sell_volume': 'sell_volume'
    }
    summary_pivot = summary_pivot.rename(columns=column_mapping)
    
    # Заполняем NaN значения нулями
    summary_pivot = summary_pivot.fillna(0)
    
    # Сортируем по количеству кошельков покупки (от большего к меньшему)
    summary_pivot = summary_pivot.sort_values('buy_wallets', ascending=False)
    
    return summary_pivot

def main():
    st.title("Solana Parser Dashboard")

    today = datetime.datetime.now().replace(microsecond=0)
    yesterday = today - datetime.timedelta(hours=24)

    if 'date_range' not in st.session_state:
        st.session_state.date_range = [yesterday, today]

    def update_date_range(start_date, end_date):
        st.session_state.date_range = [start_date, end_date]

    st.sidebar.subheader("Быстрый выбор дат")
    if st.sidebar.button("Последние 24 часа"):
        update_date_range(today - datetime.timedelta(hours=24), today)
    if st.sidebar.button("Последние 3 дня"):
        update_date_range(today - datetime.timedelta(days=2), today)
    if st.sidebar.button("Последние 7 дней"):
        update_date_range(today - datetime.timedelta(days=6), today)
    if st.sidebar.button("Текущий месяц"):
        update_date_range(today.replace(day=1), today)
    if st.sidebar.button("Все время"):
        update_date_range(datetime.datetime(2000, 1, 1), today)

    date_range = st.sidebar.date_input("Выберите диапазон дат", st.session_state.date_range)

    if date_range != st.session_state.date_range:
        st.session_state.date_range = date_range

    if len(date_range) == 2:
        date_from, date_to = date_range
        date_from = pd.Timestamp(date_from)
        date_to = pd.Timestamp(date_to) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)

        conn = get_connection()
        df = fetch_data(conn, date_from, date_to)

        st.subheader(f"Сводная информация по монетам с {date_from.date()} по {date_to.date()}")
        summary_df = create_summary_table(df)
        
        # Добавляем столбец с чекбоксами
        summary_df.insert(0, 'Select', False)
        
        # Отображаем таблицу с возможностью редактирования
        edited_df = st.data_editor(
            summary_df,
            column_config={
                "Select": st.column_config.CheckboxColumn(label="Выбрать"),
                "coin": "Монета",
                "buy_wallets": st.column_config.NumberColumn("Кошельки (покупка)"),
                "sell_wallets": st.column_config.NumberColumn("Кошельки (продажа)"),
                "buy_volume": st.column_config.NumberColumn("Объем покупок", format="$.2f"),
                "sell_volume": st.column_config.NumberColumn("Объем продаж", format="$.2f")
            },
            disabled=["coin", "buy_wallets", "sell_wallets", "buy_volume", "sell_volume"],
            hide_index=True,
            use_container_width=True
        )

        # Получаем выбранные монеты
        selected_coins = edited_df[edited_df['Select']]['coin'].tolist()

        st.subheader(f"Детальные данные с {date_from.date()} по {date_to.date()}")
        if selected_coins:
            filtered_df = df[(df['swapped_currency'].isin(selected_coins)) | (df['received_currency'].isin(selected_coins))]
            st.dataframe(filtered_df, use_container_width=True)
        else:
            st.dataframe(df, use_container_width=True)

    else:
        st.error("Пожалуйста, выберите диапазон дат.")

if __name__ == "__main__":
    main()
