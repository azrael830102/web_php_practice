import datetime
import requests
import pandas as pd
import pymysql

db_settings = {
    "host": "127.0.0.1",
    "port": 3308,
    "user": "root",
    "password": "",
    "db": "practice",
    "charset": "utf8"
}
print("stock records refreshing")
try:
    conn = pymysql.connect(**db_settings)
    with conn.cursor() as cursor:
        command = "SELECT * FROM tbl_stock"
        cursor.execute(command)
        result = cursor.fetchone()
        nowDate = datetime.date.today()
        refresh = False
        if result:
            # update table
            field_names = [i[0] for i in cursor.description]
            updateDate = result[field_names.index('update_date')]
            if nowDate.day - updateDate.day:
                refresh = True
                command = "DELETE FROM tbl_stock"
                cursor.execute(command)
        if not result or refresh:
            # refresh record
            res = requests.get("http://isin.twse.com.tw/isin/C_public.jsp?strMode=2")
            df = pd.read_html(res.text)[0]
            command = "INSERT INTO tbl_stock (stock_id, stock_name, isin_code, listing_date, industry, update_date) " \
                      "VALUES (%(id)s, %(name)s, %(isin_code)s, %(listing_date)s, %(industry)s, %(update_date)s) "
            for row in df.itertuples(name='Stock', index=False):
                if ('\u3000' in row[0]) and (isinstance(row[4], str)):
                    s = row[0].split('\u3000')
                    values = {
                        'id': s[0],
                        'name': s[1],
                        'isin_code': row[1],
                        'listing_date': row[2],
                        'industry': row[4],
                        'update_date': nowDate
                    }
                    cursor.execute(command, values)
except Exception as ex:
    print("error")
    print(ex)

print("stock records refreshed")
