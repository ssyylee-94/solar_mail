import pandas as pd
import joblib
import connection
import settings
import sendemail


def solarmail_connection():
    db_setting = connection.SolarDB()
    engine = db_setting.engine
    con = db_setting.conn
    return engine, con


def kma_connection():
    db_setting = connection.KmaDB()
    engine = db_setting.engine
    con = db_setting.conn
    return engine, con


def input_data():
    date = input('enter date for forecasting: ')
    return date


def load_model(region):
    model = joblib.load('xgb-model-{}5.dat'.format(region))
    return model


def get_pysolar(date, region):
    #  check connection first,
    engine, con = solarmail_connection()

    if con.invalidated:
        con.connect()
    else:
        pysolar_df = pd.read_sql('select * from pysolar_{} WHERE tDate ="{}"'.format(region, date), con, index_col=None)
        pysolar_df = pysolar_df[['time', 'tDeal', 'altitude', 'radiation']]
        pysolar_df['tYear'] = pysolar_df.time.dt.year
        pysolar_df['tDoY'] = pysolar_df.time.dt.dayofyear

    con.close()
    engine.dispose()

    return pysolar_df


def get_weather(date, region):

    engine, con = kma_connection()

    if con.invalidated:
        con.connect()
    else:
        weather = pd.read_sql('select Target, T3H, REH, WSD, SKY from village_fcst WHERE Region = "{}" AND DATE(Target) = "{}"'.format(region, date),
                              con, index_col=None)
        weather.drop_duplicates(subset='Target', keep='last', inplace=True)

        weather = weather.reset_index(drop=True)
        weather.rename(columns={'T3H': 'temperature', 'REH': 'humidity', 'WSD': 'wind', 'SKY': 'sky'}, inplace=True)
        # T3H: 기온, REH: 습도,  WSD: 풍속, SKY: 운량 !~4

    con.close()
    engine.dispose()

    return weather


def make_input(date, region, region_kor):
    solar_df = get_pysolar(date, region)
    weather_df = get_weather(date, region_kor)

    solar_df = solar_df.join(weather_df.set_index("Target"), on='time')

    solar_df.interpolate(method='linear', inplace=True)

    solar_df = solar_df[['tDeal', 'tDoY', 'tYear', 'altitude', 'radiation', 'temperature', 'humidity', 'wind', 'sky']]

    return solar_df


def predict_gen(date, region, region_kor):
    model = load_model(region)
    solar_df = make_input(date, region, region_kor)
    predict_ = model.predict(solar_df)

    predict = pd.DataFrame(data=predict_, columns=['tGen'])
    num = predict._get_numeric_data()
    num[num < 0] = 0

    solar_df['tGen'] = predict

    solar_df.loc[0:5, 'tGen'] = 0
    solar_df.loc[19:23, 'tGen'] = 0

    return solar_df


def get_asset():
    engine, con = solarmail_connection()

    region_kor = []

    if con.invalidated:
        con.connect()
    else:
        asset = pd.read_sql('select * from asset', con, index_col=None)
        region_list = asset.region.unique()

    for region in region_list:
        region_kor.append(settings.region_dict[region])

    return region_list, region_kor, asset


def total_prediction(date):
    region_list, region_kor, asset = get_asset()

    result = {}

    for i in range(len(region_list)):
        result[region_list[i]] = predict_gen(date, region_list[i], region_kor[i])

    return result, asset


def get_user():
    engine, con = solarmail_connection()
    if con.invalidated:
        con.connect()
    else:
        user_df = pd.read_sql('select * from user', con, index_col=None)
    con.close()
    engine.dispose()
    return user_df


def save_result(result, asset):
    user_df = get_user()

    for i in range(len(user_df)):

        _id_title = 'user_' + str(user_df.iloc[i,].id)

        _asset = asset[asset.user_id == user_df.iloc[i,].id]
        _asset = _asset.reset_index(drop=True)

        for j in range(len(_asset)):
            use = result[_asset.iloc[j,].region]
            use['kWh'] = use['tGen'] * _asset.iloc[j,]['size']
            use = use[['tGen', 'kWh']]

            if j == 0:
                with pd.ExcelWriter('save_excel/{}.xlsx'.format(_id_title)) as writer:
                    use.to_excel(writer, sheet_name=str(_asset.iloc[j,].assetname), encoding='utf-8', index=False)
            else:
                with pd.ExcelWriter('save_excel/{}.xlsx'.format(_id_title), mode='a') as writer:
                    use.to_excel(writer, sheet_name=str(_asset.iloc[j,].assetname), encoding='utf-8', index=False)
    return user_df


def send_xlsx(date, user_df):

    for i in range(len(user_df)):
        address = user_df.iloc[i, ].email

        try:
            attachment_file = "save_excel/user_{}.xlsx".format(user_df.iloc[i,].id)
            subject_title = "발전량 예측 결과 메일"
            content_line = date + "의 예측 결과"
            sendemail.send_mail(addr=address, subj_layout=subject_title, cont_layout=content_line, attachment=attachment_file)
        except FileNotFoundError as e:
            pass

    # print("메일 전송 완료!!!!!!!!!!")


def solarmail_func():

    pred_date = input_data()
    result_dict, asset_df = total_prediction(pred_date)
    user_data = save_result(result_dict, asset_df)

    send_xlsx(pred_date, user_data)


if __name__ == "__main__":

    pred_date = input_data()
    result_dict, asset_df = total_prediction(pred_date)
    user_data = save_result(result_dict, asset_df)

    send_xlsx(pred_date, user_data)
