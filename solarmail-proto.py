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


def get_kma(date, region):

    engine, con = kma_connection()

    if con.invalidated:
        con.connect()
    else:
        temp = pd.read_sql('select * from KMA_동네예보_3시간기온 WHERE city = "{}" AND DATE(Target) = "{}"'.format(region, date), con, index_col=None)
        temp.drop_duplicates(subset='Target', keep='last', inplace=True)
        temp = temp.reset_index(drop=True)
        temp = temp[['Target', 'Value']]
        temp.rename(columns={'Value': 'temperature'}, inplace=True)

        humid = pd.read_sql('select * from KMA_동네예보_습도 WHERE city = "{}" AND DATE(Target) = "{}"'.format(region, date), con, index_col=None)
        humid.drop_duplicates(subset='Target', keep='last', inplace=True)
        humid = humid.reset_index(drop=True)
        humid = humid[['Target', 'Value']]
        humid.rename(columns={'Value': 'humidity'}, inplace=True)

        wind = pd.read_sql('select * from KMA_동네예보_풍속 WHERE city = "{}" AND DATE(Target) = "{}"'.format(region, date), con, index_col=None)
        wind.drop_duplicates(subset='Target', keep='last', inplace=True)
        wind = wind.reset_index(drop=True)
        wind = wind[['Target', 'Value']]
        wind.rename(columns={'Value': 'wind'}, inplace=True)

        sky = pd.read_sql('select * from KMA_동네예보_하늘상태 WHERE city = "{}" AND DATE(Target) = "{}"'.format(region, date), con, index_col=None)
        sky.drop_duplicates(subset='Target', keep='last', inplace=True)
        sky = sky.reset_index(drop=True)
        sky = sky[['Target', 'Value']]
        sky.rename(columns={'Value': 'sky'}, inplace=True)

    con.close()
    engine.dispose()

    return temp, humid, wind, sky


def get_weather(date, region):
    temp, humid, wind, sky = get_kma(date, region)

    weather = pd.merge(temp, humid, on='Target', how='outer')
    weather = pd.merge(weather, wind, on='Target', how='outer')
    weather = pd.merge(weather, sky, on='Target', how='outer')

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


def save_result(result, asset):

    for i in range(len(asset)):
        use = result[asset.iloc[i, ].region]
        use['kWh'] = use['tGen'] * asset.iloc[i, ].size
        use['assetname'] = asset.iloc[i, ].assetname

        file_name = str(asset.iloc[i, ].assetname)

        use.to_excel('save_excel/{}.xlsx'.format(file_name), encoding='utf-8', index=False)


if __name__ == "__main__":

    pred_date = input_data()
    result_dict, asset_df = total_prediction(pred_date)
    save_result(result_dict, asset_df)


    """
    user_df = pd.read_sql("select * from user", con, index_col=None)  # 사용자 정보 로드, id/ username/ email
    asset_df = pd.read_sql("select * from asset", con, index_col=None)  # region/ address/ size(MW)/resource/ user_id
    gps_df = pd.read_sql("select * from gps", con, index_col=None)  # 일기예보 지역별 gps 좌표
    weather_df = pd.read_sql("select * from weather", con, index_col=None)  # 기상청 예보

    result_df = pd.DataFrame([])

    altitude, radiation

    input_df = kma_df[['tDeal', 'tDoY', 'tYear', 'altitude', 'radiation', 'temperature', 'humidity', 'wind', 'sky']]

    predictions = model.predict(input_df)

    predictions = model.predict()
    """
