import csv
import webScrappingYahoo as webScrapping
import json
import threading
import websocket
import datetime
import hmac
import hashlib
import base64
import threading
import time
import pandas as pd
import requests
from GoogleSpreadsheets import update_row


def run():
    
    def envio_msjIMG(symbol, time1, time2):
        r = requests.post(f'https://api.telegram.org/bot5363622455:AAEoFyCSHrhOZhQzxSGD6MxOWKw9h9aoQqY/sendPhoto',
                        files = {'photo':(r'table.png', open(r'table.png', 'rb'))},
                        data={'chat_id': '-614042061', 'parse_mode':'markdown','caption': '*{}* Analysis from {} to {} '.format(symbol,time1,time2)})
        return r.json()
    #Prices extracted from WebScrapping
    global current_prices, usd_worth
    c = webScrapping.calcula_usd()
    current_prices = c[0]
    usd_worth = c[1]
    print(current_prices)
    

    """## Talos WS"""

    api_key = "OSLX2YE2LN4A"
    api_secret = "gfdry6mbtn44o91q2r8xmlrv6dkgrbbo"
    utc_now = datetime.datetime.utcnow()
    utc_datetime = utc_now.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
    host = "tal-75.prod.talostrading.com" # tal-1.prod.talostrading.com, for example
    path = "/ws/v1"
    params = "\n".join([
        "GET",
        utc_datetime,
        host,
        path,
    ])
    hash = hmac.new(
        api_secret.encode('ascii'), params.encode('ascii'), hashlib.sha256)
    hash.hexdigest()
    signature = base64.urlsafe_b64encode(hash.digest()).decode()
    header = {
        "TALOS-KEY": api_key,
        "TALOS-SIGN": signature,
        "TALOS-TS": utc_datetime,
    }

    spreads_BTCUSD = []
    spreads_ETHUSD = []
    spreads_LTCUSD = []
    spreads_BCHUSD = []
    spreads_USDTUSD = []
    spreads_AAVEUSD = []
    spreads_COMPUSD = []
    spreads_DAIUSD = []
    spreads_ENJUSD = []
    spreads_GRTUSD = []
    spreads_LINKUSD = []
    spreads_MANAUSD = []
    spreads_MATICUSD = []
    spreads_OGNUSD = []
    spreads_SNXUSD = []
    spreads_SUSHIUSD = []
    spreads_UNIUSD = []
    spreads_YFIUSD = []

    spreads= []
    d1_eth  = []
    d1_btc =  []
    d1_ltc =  []
    d1_bch =  []
    d1_usdt =  []
    d1_aave =  []
    d1_band =  []
    d1_bat=  []
    d1_comp =  []
    d1_dai =  []
    d1_enj =  []
    d1_grt =  []
    d1_link =  []
    d1_mana =  []
    d1_matic =  []
    d1_ogn =  []
    d1_snx =  []
    d1_sushi =  []
    d1_uni =  []
    d1_yfi =  []

    global df_eth, df_btc, df_ltc, df_bch, df_usdt, df_aave, df_band, df_bat, df_comp, df_dai, df_enj, df_grt, df_link, df_mana, df_matic, df_ogn, df_snx, df_sushi, df_uni, df_yfi

    df_eth = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_btc = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_ltc = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_bch = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_usdt = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_aave = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_band = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_bat = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_comp = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_dai = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_enj = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_grt = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_link = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_mana = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_matic = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_ogn = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_snx = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_sushi = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_uni = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    df_yfi = pd.DataFrame({"Bid": [], "Ask" : [], "Size":[], "Time": []})
    
    def ws_open(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "BTC-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["BTC"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open1(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "ETH-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["ETH"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open2(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "LTC-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["LTC"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open3(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "BCH-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["BCH"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open4(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "USDT-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["USDT"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open5(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "AAVE-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets":[str(current_prices["AAVE"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open6(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "BAND-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["BAND"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open7(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "BAT-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["BAT"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open8(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "COMP-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["COMP"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open9(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "DAI-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["DAI"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open10(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "ENJ-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["ENJ"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
        
    def ws_open11(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "GRT-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["GRT"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open12(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "LINK-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["LINK"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open13(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "MANA-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["MANA"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open14(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "MATIC-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["MATIC"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open15(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "OGN-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["OGN"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open16(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "SUSHI-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["SUSHI"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open17(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "UNI-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["UNI"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open18(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "SNX-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["SNX"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))
    
    def ws_open19(ws):
        ws.send(json.dumps({
        "reqid": 5,
        "type": "subscribe",
        "streams": [
            {
            "name": "MarketDataSnapshot",
            "Symbol": "YFI-USD",
            "DepthType": "Price",
            "Markets": ["osl_am"],
            "SizeBuckets": [str(current_prices["YFI"])]
            }
        ],
        "ts": "2022-07-01T04:53:00.000Z"
        }))


    def on_message(ws, message):
  
        response = json.loads(message)
        print(response)
        time = response['ts']
        
        if( len(response['data'][0]['Bids']) <= 6 and len(response['data'][0]['Offers']) <=6 ):
            #--------------1-----------------
            bid_price1 = float(response['data'][0]['Bids'][0]['Price'])
            size1 = response['data'][0]['Bids'][0]['Size']
            offer_price1 = float(response['data'][0]['Offers'][0]['Price'])
           

            if response['data'][0]['Symbol'] == 'BTC-USD':
                
                d1_btc.append((bid_price1, offer_price1, size1, time))
                
            elif response['data'][0]['Symbol'] == 'ETH-USD':

                d1_eth.append((bid_price1, offer_price1, size1, time))
               
            
            elif response['data'][0]['Symbol'] == 'LTC-USD':
                d1_ltc.append((bid_price1, offer_price1, size1, time))
               
                
            elif response['data'][0]['Symbol'] == 'BCH-USD':
                d1_bch.append((bid_price1, offer_price1, size1, time))
               
            
            elif response['data'][0]['Symbol'] == 'USDT-USD':
                d1_usdt.append((bid_price1, offer_price1, size1, time))
                
            elif response['data'][0]['Symbol'] == 'AAVE-USD':
                d1_aave.append((bid_price1, offer_price1, size1, time))
               
            elif response['data'][0]['Symbol'] == 'BAND-USD':
                d1_band.append((bid_price1, offer_price1, size1, time))
               
                
            elif response['data'][0]['Symbol'] == 'BAT-USD':
                d1_bat.append((bid_price1, offer_price1, size1, time))
              
                
            elif response['data'][0]['Symbol'] == 'COMP-USD':
                d1_comp.append((bid_price1, offer_price1, size1, time))
               
                
            elif response['data'][0]['Symbol'] == 'DAI-USD':
                d1_dai.append((bid_price1, offer_price1, size1, time))
               
            
            elif response['data'][0]['Symbol'] == 'ENJ-USD':
                d1_enj.append((bid_price1, offer_price1, size1, time))
                
            
            elif response['data'][0]['Symbol'] == 'GRT-USD':
                d1_grt.append((bid_price1, offer_price1, size1, time))
               
            elif response['data'][0]['Symbol'] == 'LINK-USD':
                d1_link.append((bid_price1, offer_price1, size1, time))
                
            elif response['data'][0]['Symbol'] == 'MANA-USD':
                d1_mana.append((bid_price1, offer_price1, size1, time))
               
            
            elif response['data'][0]['Symbol'] == 'MATIC-USD':
                d1_matic.append((bid_price1, offer_price1, size1, time))
               
            elif response['data'][0]['Symbol'] == 'OGN-USD':
                d1_ogn.append((bid_price1, offer_price1, size1, time))
                
            
            elif response['data'][0]['Symbol'] == 'SNX-USD':
                d1_snx.append((bid_price1, offer_price1, size1, time))
            
            elif response['data'][0]['Symbol'] == 'SUSHI-USD':
                d1_sushi.append((bid_price1, offer_price1, size1, time))
                
            
            elif response['data'][0]['Symbol'] == 'UNI-USD':
                d1_uni.append((bid_price1, offer_price1, size1, time))
                
            
            elif response['data'][0]['Symbol'] == 'YFI-USD':
                d1_yfi.append((bid_price1, offer_price1, size1, time))
               
                

    ws = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open, on_message = on_message )
    ws1 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open1 , on_message = on_message )  
    ws2 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open2 , on_message = on_message )  
    ws3 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open3 , on_message = on_message )  
    ws4 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open4 , on_message = on_message )  
    ws5 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open5 , on_message = on_message )  
    ws6 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open6 , on_message = on_message )  
    ws7 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open7 , on_message = on_message )  
    ws8 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open8 , on_message = on_message )  
    ws9 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open9 , on_message = on_message )  
    ws10 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open10 , on_message = on_message )  
    ws11 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open11 , on_message = on_message )  
    ws12 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open12 , on_message = on_message )  
    ws13 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open13 , on_message = on_message )  
    ws14 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open14 , on_message = on_message )  
    ws15 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open15 , on_message = on_message )  
    ws16 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open16 , on_message = on_message )  
    ws17 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open17 , on_message = on_message )  
    ws18 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open18 , on_message = on_message )  
    ws19 = websocket.WebSocketApp("wss://"+host+path, header = header, on_open = ws_open19 , on_message = on_message )  

    def wsthread(spreads):
        ws.run_forever()
            
    def wsthread1(spreads):
        ws1.run_forever()
    
    def wsthread2(spreads):
        ws2.run_forever()
        
    def wsthread3(spreads):
        ws3.run_forever()
    
    def wsthread4(spreads):
        ws4.run_forever()
    
    def wsthread5(spreads):
        ws5.run_forever()
        
    def wsthread6(spreads):
        ws6.run_forever()
        
    def wsthread7(spreads):
        ws7.run_forever()
    
    def wsthread8(spreads):
        ws8.run_forever()
    
    def wsthread9(spreads):
        ws9.run_forever()
    
    def wsthread10(spreads):
        ws10.run_forever()
    
    def wsthread11(spreads):
        ws11.run_forever()
        
    def wsthread12(spreads):
        ws12.run_forever()
    
    def wsthread13(spreads):
        ws13.run_forever()
    
    def wsthread14(spreads):
        ws14.run_forever()
    
    def wsthread15(spreads):
        ws15.run_forever()
    
    def wsthread16(spreads):
        ws16.run_forever()
    
    def wsthread17(spreads):
        ws17.run_forever()
        
    def wsthread18(spreads):
        ws18.run_forever()
        
    def wsthread19(spreads):
        ws19.run_forever()
        
    t = threading.Thread(target=wsthread,args=(spreads,))  
    t.start()
    t1 = threading.Thread(target=wsthread1,args=(spreads,))
    t1.start()
    t2 = threading.Thread(target=wsthread2,args=(spreads,))
    t2.start()
    t3 = threading.Thread(target=wsthread3,args=(spreads,))
    t3.start()
    t4 = threading.Thread(target=wsthread4,args=(spreads,))
    t4.start()
    t5 = threading.Thread(target=wsthread5,args=(spreads,))
    t5.start()
    t6 = threading.Thread(target=wsthread6,args=(spreads,))
    t6.start()
    t7 = threading.Thread(target=wsthread7,args=(spreads,))
    t7.start()
    t8 = threading.Thread(target=wsthread8,args=(spreads,))
    t8.start()
    t9 = threading.Thread(target=wsthread9,args=(spreads,))
    t9.start()
    t10 = threading.Thread(target=wsthread10,args=(spreads,))
    t10.start()
    t11 = threading.Thread(target=wsthread11,args=(spreads,))
    t11.start()
    t12 = threading.Thread(target=wsthread12,args=(spreads,))
    t12.start()
    t13 = threading.Thread(target=wsthread13,args=(spreads,))
    t13.start()
    t14 = threading.Thread(target=wsthread14,args=(spreads,))
    t14.start()
    t15 = threading.Thread(target=wsthread15,args=(spreads,))
    t15.start()
    t16 = threading.Thread(target=wsthread16,args=(spreads,))
    t16.start()
    t17 = threading.Thread(target=wsthread17,args=(spreads,))
    t17.start()
    t18 = threading.Thread(target=wsthread18,args=(spreads,))
    t18.start()
    t19 = threading.Thread(target=wsthread18,args=(spreads,))
    t19.start()


    time.sleep(180) #----------------------------------------------------------------------Cambiar
    
    ws.close()
    ws1.close()
    ws2.close()
    ws3.close()
    ws4.close()
    ws5.close()
    ws6.close()
    ws7.close()
    ws8.close()
    ws9.close()
    ws10.close()
    ws11.close()
    ws12.close()
    ws13.close()
    ws14.close()
    ws15.close()
    ws16.close()
    ws17.close()
    ws18.close()
    ws19.close()

    datos_generales = []
    #-----------BTC-----------

    df1_btc = pd.DataFrame(d1_btc, columns=('Bid', 'Ask', 'Size','Time'))
    frames = [df1_btc]
    spreads_BTCUSD = pd.concat(frames)
    
    if spreads_BTCUSD.empty == False:

        time1 = spreads_BTCUSD['Time'].iloc[[0][0]]
        time1_ = time1[11:19]
        print(time1)
        time2 = spreads_BTCUSD['Time'].iloc[[-1][0]]
        time2_ = time2[11:19]
        print(time2)
        spreads_BTCUSD = spreads_BTCUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_BTCUSD["Spreads"] = ((spreads_BTCUSD['Ask'] - spreads_BTCUSD['Bid'])/spreads_BTCUSD['Ask'])*10000
        dataframe_BTC = spreads_BTCUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_BTC,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('BTC-USD',time1, time2)
        mean =  dataframe_BTC.iloc[0][1]
        size = str(current_prices["BTC"])
        usd = str(usd_worth["BTC"])
        update_row("BTC",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["BTC", size, usd, mean, "EXCH"])
    
    #-----------ETH-----------
    time.sleep(5)
    
    
    df1_eth = pd.DataFrame(d1_eth, columns=('Bid', 'Ask', 'Size','Time'))
    
    frames = [df1_eth]
    spreads_ETHUSD = pd.concat(frames)
    
    if spreads_ETHUSD.empty == False:
        time1 = spreads_ETHUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_ETHUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_ETHUSD = spreads_ETHUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_ETHUSD["Spreads"] = ((spreads_ETHUSD['Ask'] - spreads_ETHUSD['Bid'])/spreads_ETHUSD['Ask'])*10000
        dataframe_ETH = spreads_ETHUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_ETH,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('ETH-USD',time1, time2)
        mean =  dataframe_ETH.iloc[0][1]
        size = str(current_prices["ETH"])
        usd = str(usd_worth["ETH"])
        update_row("ETH",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["ETH", size, usd, mean, "EXCH"])
    
    
    
    #-----------USDT-----------
    time.sleep(5)

    df1_usdt = pd.DataFrame(d1_usdt, columns=('Bid', 'Ask', 'Size','Time'))
    frames = [df1_usdt]
    spreads_USDTUSD = pd.concat(frames)
    
    if spreads_USDTUSD.empty == False:

        time1 = spreads_USDTUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_USDTUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_USDTUSD = spreads_USDTUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_USDTUSD["Spreads"] = ((spreads_USDTUSD['Ask'] - spreads_USDTUSD['Bid'])/spreads_USDTUSD['Ask'])*10000
        dataframe_USDTUSD = spreads_USDTUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_USDTUSD,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('USDT-USD',time1, time2)
        mean =  dataframe_USDTUSD.iloc[0][1]
        size = str(current_prices["USDT"])
        usd = str(usd_worth["USDT"])
        update_row("USDT",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["USDT", size, usd, mean, "EXCH"])
    
    #-----------BCH-----------
    time.sleep(5)
    df1_bch = pd.DataFrame(d1_bch, columns=('Bid', 'Ask', 'Size','Time'))
    
    frames = [df1_bch]
    spreads_BCHUSD = pd.concat(frames)
    
    if spreads_BCHUSD.empty == False:

        time1 = spreads_BCHUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_BCHUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_BCHUSD = spreads_BCHUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_BCHUSD["Spreads"] = ((spreads_BCHUSD['Ask'] - spreads_BCHUSD['Bid'])/spreads_BCHUSD['Ask'])*10000
        dataframe_BCHUSD = spreads_BCHUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_BCHUSD,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('BCH-USD',time1, time2)
        mean =  dataframe_BCHUSD.iloc[0][1]
        size = str(current_prices["BCH"])
        usd = str(usd_worth["BCH"])
        update_row("BCH",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["BCH", size, usd, mean, "EXCH"])
    
    time.sleep(1)
    
        
    #-----------LTC-----------
    time.sleep(5)
    df1_ltc = pd.DataFrame(d1_ltc, columns=('Bid', 'Ask', 'Size','Time'))
    frames = [df1_ltc]
    spreads_LTCUSD = pd.concat(frames)
    
    if spreads_LTCUSD.empty == False:

        time1 = spreads_LTCUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_LTCUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_LTCUSD = spreads_LTCUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_LTCUSD["Spreads"] = ((spreads_LTCUSD['Ask'] - spreads_LTCUSD['Bid'])/spreads_LTCUSD['Ask'])*10000
        dataframe_LTCUSD = spreads_LTCUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_LTCUSD,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('LTC-USD',time1, time2)
        mean =  dataframe_LTCUSD.iloc[0][1]
        size = str(current_prices["LTC"])
        usd = str(usd_worth["LTC"])
        update_row("LTC",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["LTC", size, usd, mean, "EXCH"])
       
    #-----------AAVE-----------
    time.sleep(5)
    df1_aave = pd.DataFrame(d1_aave, columns=('Bid', 'Ask', 'Size','Time'))
    frames = [df1_aave]
    spreads_AAVEUSD = pd.concat(frames)
    if spreads_AAVEUSD.empty == False:

        time1 = spreads_AAVEUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_AAVEUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_AAVEUSD = spreads_AAVEUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_AAVEUSD["Spreads"] = ((spreads_AAVEUSD['Ask'] - spreads_AAVEUSD['Bid'])/spreads_AAVEUSD['Ask'])*10000
        dataframe_AAVEUSD = spreads_AAVEUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_AAVEUSD,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('AAVE-USD',time1, time2)
        mean =  dataframe_AAVEUSD.iloc[0][1]
        size = str(current_prices["AAVE"])
        usd = str(usd_worth["AAVE"])
        update_row("AAVE",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["AAVE", size, usd, mean, "EXCH"])
    
    time.sleep(5)
    #-----------BAND-----------

    df1_band = pd.DataFrame(d1_band, columns=('Bid', 'Ask', 'Size','Time'))
    frames = [df1_band]
    spreads_BANDUSD = pd.concat(frames)
    
    if spreads_BANDUSD.empty == False:
        time1 = spreads_BANDUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_BANDUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_BANDUSD = spreads_BANDUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_BANDUSD["Spreads"] = ((spreads_BANDUSD['Ask'] - spreads_BANDUSD['Bid'])/spreads_BANDUSD['Ask'])*10000
        dataframe_BANDUSD = spreads_BANDUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_BANDUSD,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('BAND-USD',time1, time2)
        mean =  dataframe_BANDUSD.iloc[0][1]
        size = str(current_prices["BAND"])
        usd = str(usd_worth["BAND"])
        update_row("BAND",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["BAND", size, usd, mean, "EXCH"])
    
    time.sleep(5)
    #-----------BAT-----------

    df1_bat= pd.DataFrame(d1_bat, columns=('Bid', 'Ask', 'Size','Time'))
    frames = [df1_bat]
    spreads_BATUSD = pd.concat(frames)
    
    if spreads_BATUSD.empty == False:
        time1 = spreads_BATUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_BATUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_BATUSD = spreads_BATUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_BATUSD["Spreads"] = ((spreads_BATUSD['Ask'] - spreads_BATUSD['Bid'])/spreads_BATUSD['Ask'])*10000
        dataframe_BATUSD = spreads_BATUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_BATUSD,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('BAT-USD',time1, time2)
        mean =  dataframe_BATUSD.iloc[0][1]
        size = str(current_prices["BAT"])
        usd = str(usd_worth["BAT"])
        update_row("BAT",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["BAT", size, usd, mean, "EXCH"])
        
    #-----------COMP-----------
    time.sleep(5)
    df1_comp = pd.DataFrame(d1_comp, columns=('Bid', 'Ask', 'Size','Time'))
    frames = [df1_comp]
    spreads_COMPUSD = pd.concat(frames)
    
    if spreads_COMPUSD.empty == False:
        time1 = spreads_COMPUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_COMPUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_COMPUSD = spreads_COMPUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_COMPUSD["Spreads"] = ((spreads_COMPUSD['Ask'] - spreads_COMPUSD['Bid'])/spreads_COMPUSD['Ask'])*10000
        dataframe_COMPUSD = spreads_COMPUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_COMPUSD,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('COMP-USD',time1, time2)
        mean =  dataframe_COMPUSD.iloc[0][1]
        size = str(current_prices["COMP"])
        usd = str(usd_worth["COMP"])
        update_row("COMP",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["COMP", size, usd, mean, "EXCH"])
        
    time.sleep(5)
    
    #-----------DAI-----------

    df1_dai = pd.DataFrame(d1_dai, columns=('Bid', 'Ask', 'Size','Time'))
    frames = [df1_dai]
    spreads_DAIUSD = pd.concat(frames)
    
    if spreads_DAIUSD.empty == False:

        time1 = spreads_DAIUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_DAIUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_DAIUSD = spreads_DAIUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_DAIUSD["Spreads"] = ((spreads_DAIUSD['Ask'] - spreads_DAIUSD['Bid'])/spreads_DAIUSD['Ask'])*10000
        dataframe_DAI = spreads_DAIUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_DAI,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('DAI-USD',time1, time2)
        mean =  dataframe_DAI.iloc[0][1]
        size = str(current_prices["DAI"])
        usd = str(usd_worth["DAI"])
        update_row("DAI",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["DAI", size, usd, mean, "EXCH"])
    
    #-----------ENJ-----------
    time.sleep(5)
    df1_enj = pd.DataFrame(d1_enj, columns=('Bid', 'Ask', 'Size','Time'))

    frames = [df1_enj]
    spreads_ENJUSD = pd.concat(frames)
    
    if spreads_ENJUSD.empty == False:

        time1 = spreads_ENJUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_ENJUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_ENJUSD = spreads_ENJUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_ENJUSD["Spreads"] = ((spreads_ENJUSD['Ask'] - spreads_ENJUSD['Bid'])/spreads_ENJUSD['Ask'])*10000
        dataframe_ENJ = spreads_ENJUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_ENJ,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('ENJ-USD',time1, time2)
        mean =  dataframe_ENJ.iloc[0][1]
        size = str(current_prices["ENJ"])
        usd = str(usd_worth["ENJ"])
        update_row("ENJ",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["ENJ", size, usd, mean, "EXCH"])
    
    time.sleep(5) 
    #-----------GRT-----------

    df1_grt = pd.DataFrame(d1_grt, columns=('Bid', 'Ask', 'Size','Time'))
   
    frames = [df1_grt]
    spreads_GRTUSD = pd.concat(frames)
    
    if spreads_GRTUSD.empty == False:

        time1 = spreads_GRTUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_GRTUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_GRTUSD = spreads_GRTUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_GRTUSD["Spreads"] = ((spreads_GRTUSD['Ask'] - spreads_GRTUSD['Bid'])/spreads_GRTUSD['Ask'])*10000
        dataframe_GRT = spreads_GRTUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_GRT,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('GRT-USD',time1, time2)
        mean =  dataframe_GRT.iloc[0][1]
        size = str(current_prices["GRT"])
        usd = str(usd_worth["GRT"])
        update_row("GRT",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["GRT", size, usd, mean, "EXCH"])
    
    #-----------LINK-----------
    time.sleep(5)
    df1_link = pd.DataFrame(d1_link, columns=('Bid', 'Ask', 'Size','Time'))
   

    frames = [df1_link]
    spreads_LINKUSD = pd.concat(frames)
    
    if spreads_LINKUSD.empty == False:

        time1 = spreads_LINKUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_LINKUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_LINKUSD = spreads_LINKUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_LINKUSD["Spreads"] = ((spreads_LINKUSD['Ask'] - spreads_LINKUSD['Bid'])/spreads_LINKUSD['Ask'])*10000
        dataframe_LINK = spreads_LINKUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_LINK,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('LINK-USD',time1, time2)
        mean =  dataframe_LINK.iloc[0][1]
        size = str(current_prices["LINK"])
        usd = str(usd_worth["LINK"])
        update_row("LINK",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["LINK", size, usd, mean, "EXCH"])
    
    #-----------MANA-----------
    time.sleep(5)
    df1_mana = pd.DataFrame(d1_mana, columns=('Bid', 'Ask', 'Size','Time'))
    
    frames = [df1_mana]
    spreads_MANAUSD = pd.concat(frames)
    
    if spreads_MANAUSD.empty == False:

        time1 = spreads_MANAUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_MANAUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_MANAUSD = spreads_MANAUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_MANAUSD["Spreads"] = ((spreads_MANAUSD['Ask'] - spreads_MANAUSD['Bid'])/spreads_MANAUSD['Ask'])*10000
        dataframe_MANA = spreads_MANAUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_MANA,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('MANA-USD',time1, time2)
        mean =  dataframe_MANA.iloc[0][1]
        size = str(current_prices["MANA"])
        usd = str(usd_worth["MANA"])
        update_row("MANA",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["MANA", size, usd, mean, "EXCH"])
    
    #-----------MATIC-----------
    time.sleep(5)
    df1_matic = pd.DataFrame(d1_matic, columns=('Bid', 'Ask', 'Size','Time'))
   
    frames = [df1_matic]
    spreads_MATICUSD = pd.concat(frames)
    
    if spreads_MATICUSD.empty == False:

        time1 = spreads_MATICUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_MATICUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_MATICUSD = spreads_MATICUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_MATICUSD["Spreads"] = ((spreads_MATICUSD['Ask'] - spreads_MATICUSD['Bid'])/spreads_MATICUSD['Ask'])*10000
        dataframe_MATIC = spreads_MATICUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_MATIC,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('MATIC-USD',time1, time2)
        mean =  dataframe_MATIC.iloc[0][1]
        size = str(current_prices["MATIC"])
        usd = str(usd_worth["MATIC"])
        update_row("MATIC",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["MATIC", size, usd, mean, "EXCH"])
    
    #-----------OGN-----------
    time.sleep(5)
    df1_ogn = pd.DataFrame(d1_ogn, columns=('Bid', 'Ask', 'Size','Time'))
    

    frames = [df1_ogn]
    spreads_OGNUSD = pd.concat(frames)
    
    if spreads_OGNUSD.empty == False:

        time1 = spreads_OGNUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_OGNUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_OGNUSD = spreads_OGNUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_OGNUSD["Spreads"] = ((spreads_OGNUSD['Ask'] - spreads_OGNUSD['Bid'])/spreads_OGNUSD['Ask'])*10000
        dataframe_OGN = spreads_OGNUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_OGN,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('OGN-USD',time1, time2)
        mean =  dataframe_OGN.iloc[0][1]
        size = str(current_prices["OGN"])
        usd = str(usd_worth["OGN"])
        update_row("OGN",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["OGN", size, usd, mean, "EXCH"])
    
    #-----------SNX-----------
    time.sleep(5)
    df1_snx = pd.DataFrame(d1_snx, columns=('Bid', 'Ask', 'Size','Time'))

    frames = [df1_snx]
    spreads_SNXUSD = pd.concat(frames)
    
    if spreads_SNXUSD.empty == False:

        time1 = spreads_SNXUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_SNXUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_SNXUSD = spreads_SNXUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_SNXUSD["Spreads"] = ((spreads_SNXUSD['Ask'] - spreads_SNXUSD['Bid'])/spreads_SNXUSD['Ask'])*10000
        dataframe_SNX = spreads_SNXUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_SNX,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('SNX-USD',time1, time2)
        mean =  dataframe_SNX.iloc[0][1]
        size = str(current_prices["SNX"])
        usd = str(usd_worth["SNX"])
        update_row("SNX",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["SNX", size, usd, mean, "EXCH"])
    
    #-----------SUSHI-----------
    time.sleep(5)
    df1_sushi = pd.DataFrame(d1_sushi, columns=('Bid', 'Ask', 'Size','Time'))
    

    frames = [df1_sushi]
    spreads_SUSHIUSD = pd.concat(frames)
    
    if spreads_SUSHIUSD.empty == False:

        time1 = spreads_SUSHIUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_SUSHIUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_SUSHIUSD = spreads_SUSHIUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_SUSHIUSD["Spreads"] = ((spreads_SUSHIUSD['Ask'] - spreads_SUSHIUSD['Bid'])/spreads_SUSHIUSD['Ask'])*10000
        dataframe_SUSHI = spreads_SUSHIUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_SUSHI,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('SUSHI-USD',time1, time2)
        mean =  dataframe_SUSHI.iloc[0][1]
        size = str(current_prices["SUSHI"])
        usd = str(usd_worth["SUSHI"])
        update_row("SUSHI",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["SUSHI", size, usd, mean, "EXCH"])
    
    #-----------UNI-----------
    time.sleep(5)
    df1_uni = pd.DataFrame(d1_uni, columns=('Bid', 'Ask', 'Size','Time'))

    frames = [df1_uni]
    spreads_UNIUSD = pd.concat(frames)
    
    if spreads_UNIUSD.empty == False:

        time1 = spreads_UNIUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_UNIUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_UNIUSD = spreads_UNIUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_UNIUSD["Spreads"] = ((spreads_UNIUSD['Ask'] - spreads_UNIUSD['Bid'])/spreads_UNIUSD['Ask'])*10000
        dataframe_UNI = spreads_UNIUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_UNI,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('UNI-USD',time1, time2)
        mean =  dataframe_UNI.iloc[0][1]
        size = str(current_prices["UNI"])
        usd = str(usd_worth["UNI"])
        update_row("UNI",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["UNI", size, usd, mean, "EXCH"])
    
    #-----------YFI-----------

    df1_yfi = pd.DataFrame(d1_yfi, columns=('Bid', 'Ask', 'Size','Time'))
   
    frames = [df1_yfi]
    spreads_YFIUSD = pd.concat(frames)
    
    if spreads_YFIUSD.empty == False:

        time1 = spreads_YFIUSD['Time'].iloc[[0][0]]
        time1 = time1[11:19]
        print(time1)
        time2 = spreads_YFIUSD['Time'].iloc[[-1][0]]
        time2 = time2[11:19]
        print(time2)
        spreads_YFIUSD = spreads_YFIUSD.astype({"Bid":"float","Ask":"float","Size":"float"})
        spreads_YFIUSD["Spreads"] = ((spreads_YFIUSD['Ask'] - spreads_YFIUSD['Bid'])/spreads_YFIUSD['Ask'])*10000
        dataframe_YFI = spreads_YFIUSD.groupby('Size')['Spreads'].agg(['count','mean'])
        import dataframe_image as dfi
        dfi.export(
            dataframe_YFI,
            "table.png",
            table_conversion="matplotlib"
        )
        #envio_msjIMG('YFI-USD',time1, time2)
        mean =  dataframe_YFI.iloc[0][1]
        size = str(current_prices["YFI"])
        usd = str(usd_worth["YFI"])
        update_row("YFI",size,usd,mean,"EXCH")
        mean_ = float(mean)
        size_ =  float(size)
        usd_ = int(usd)
        mean = f'{mean_:,.2f}'
        size = f'{size_:,.2f}'
        usd = f'{usd_:,}'
        datos_generales.append(["YFI", size, usd, mean, "EXCH"])
    
    df_resumen_general = pd.DataFrame(datos_generales, columns=('Coin', 'Size', 'USD_Worth','Spread','Via'))
    print(df_resumen_general)
    dfi.export(
            df_resumen_general,
            "table.png",
            table_conversion="matplotlib"
        )
    envio_msjIMG('Spreads EXCHANGE', time1_, time2_)
    