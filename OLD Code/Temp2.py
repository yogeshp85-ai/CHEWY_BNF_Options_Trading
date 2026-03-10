
from tkinter import *
from tkinter import ttk
import sqlite3
import threading
from datetime import datetime, timedelta
from kiteconnect import KiteConnect

root=Tk()
root.geometry("720x400")
root.config(background="black")
style= ttk.Style()
style.theme_use('winnative')
stopPos=False
stopStrat=False
def startThread(thread):
    match thread:
        case 0:
            t1=threading.Thread(target=pnl)
            t1.start()
        case 1:
            t1=threading.Thread(target=addStrategy)
            t1.start()
def stopThread(thread):
    global stopPos,stopStrat
    match thread:
        case 0:
            stopPos=True
        case 1:
            stopStrat=True
def connectZerodha():
    global kite
    kite=KiteConnect(api_key="")
    request_token=entryToken.get()
    data=kite.generate_session(request_token,api_secret="")
    kite.set_access_token(data["access_token"])
    top.destroy()
    username["text"]=(kite.profile()["user_name"])
def popup():
    global top,entryToken
    top=Toplevel(root)
    entryToken=ttk.Entry(top)
    entryToken.grid(row=0,column=0)	
    Button(top,text="SUBMIT",command=connectZerodha).grid(row=0,column=1)
def placeOrder(stockName):
    order_param_single = [{
        "exchange": "NSE",
        "tradingsymbol":stockName,
        "transaction_type": "BUY",
        "variety": "CO",
        "product": "MIS",
        "order_type": "MARKET",
        "quantity": 1
        }]
    margin_detail = kite.order_margins(order_param_single)
    qty=(int)((float)(capital.get())/margin_detail[0]["total"])
    sl=(float)(loss.get())/qty
    instruments="NSE:"+stockName
    quotes=kite.quote(instruments)
    if(radio2.get()==1):
        sl=quotes["NSE:"+stockName]["last_price"]-sl
    else:
        sl=quotes["NSE:"+stockName]["last_price"]+sl 
    sl=(float)("{:.2f}".format(sl))
    sl=((int)(sl*100)-((int)(sl*100))%5)/100
    orderType=kite.ORDER_TYPE_MARKET
    transType=kite.TRANSACTION_TYPE_BUY
    if(radio.get()==2):
        orderType=kite.ORDER_TYPE_LIMIT
    if(radio2.get()==2):
        transType=kite.TRANSACTION_TYPE_SELL
    
    
    limitPrice=quotes["NSE:"+stockName]["last_price"]
    kite.place_order(
    variety=kite.VARIETY_CO,
    exchange=kite.EXCHANGE_NSE,
    tradingsymbol=stockName,
    transaction_type=transType,
    quantity=qty,
    product=kite.PRODUCT_MIS,
    order_type=orderType,
    validity=kite.VALIDITY_DAY,
    trigger_price=sl,
    price=limitPrice
    )
    
def position():
    orders=kite.orders()
    qty=0
    pnl=0
    avgPrice=0
    i=0
    for row in orders:
        if(row["status"]=="TRIGGER PENDING"):
            qty=row["quantity"]
            if(row["transaction_type"]=="BUY"):
                qty=-qty
            avgPrice=averagePrice(row["parent_order_id"],orders)
            Label(positionFrame,text=row["order_id"],width=10,bg="cornsilk3",fg="black",font=("Arial Black",10)).grid(row=i,column=0)
            Label(positionFrame,text=row["tradingsymbol"],width=10,bg="cornsilk3",fg="black",font=("Arial Black",10)).grid(row=i,column=1)
            Label(positionFrame,text=qty,width=10,bg="cornsilk3",fg="black",font=("Arial Black",10)).grid(row=i,column=2)
            Label(positionFrame,text=avgPrice,width=10,bg="cornsilk3",fg="black",font=("Arial Black",10)).grid(row=i,column=3)
            Label(positionFrame,text="0",width=10,bg="cornsilk3",fg="black",font=("Arial Black",10)).grid(row=i,column=4)
            Label(positionFrame,text="0",width=10,bg="cornsilk3",fg="black",font=("Arial Black",10)).grid(row=i,column=5)
            Button(positionFrame,text="EXIT",width=10,bg="red",fg="white",font=("Arial Black",10),command=lambda:exitOrder(row["order_id"])).grid(row=i,column=6)
            i+=1
def averagePrice(parentid,order):
    for row in order:
        if((row["order_id"])==parentid):
            return row["average_price"]
    
def clearWidgets():
    widgets=positionFrame.winfo_children()
    for widget in widgets:
        widget.destroy()
def exitOrder(order_id):
    kite.cancel_order(kite.VARIETY_CO, order_id, parent_order_id=None)
    stopThread()
    clearWidgets()
    position()
def pnl():
    widgets=positionFrame.winfo_children()
    instruments=[]
    i=0
    
    while(i<(len(widgets))/7):
        instruments.append("NSE:"+widgets[i*7+1]["text"])
        i+=1
    avgPrice=0
    qty=0
    lastPrice=0
    
    i=0
    profit=0
    while(True):
        quote=(kite.quote(instruments)) 
        i=0
        overallProfit=0
        global stopPos
        if(stopPos==True):
            stopPos=False
            break
        widgetNum=0
        while(i<(len(widgets))/7):
            avgPrice=(float)(widgets[widgetNum+3]["text"])
            qty=(int)(widgets[widgetNum+2]["text"])
            lastPrice=quote[instruments[i]]["last_price"]
            profit=(lastPrice-avgPrice)*qty
            overallProfit+=profit
            widgets[widgetNum+5]["text"]="{:.2f}".format(profit)
            widgets[widgetNum+4]["text"]=lastPrice
            if(profit>0):
                widgets[widgetNum+5].config(fg="green")
            else:
                widgets[widgetNum+5].config(fg="red")
            i+=1
            widgetNum+=7
        profitLabel["text"]="{:.2f}".format(overallProfit)
        if(overallProfit>0):
            profitLabel.config(fg="green")
        else:
            profitLabel.config(fg="red")
def addStrategy():
    global stratFrame
    straButton["state"]="disabled"
    stratFrame=Frame(root)
    canvas=Canvas(stratFrame)
    scrollbar=ttk.Scrollbar(stratFrame,orient="vertical",command=canvas.yview)
    scrollableFrame=ttk.Frame(canvas)
    scrollableFrame.bind("<Configure>",lambda e:canvas.configure(scrollregion=canvas.bbox("all")))
    canvas.create_window((0,0),window=scrollableFrame,anchor="nw")
    canvas.configure(width=700,yscrollcommand=scrollbar.set)
    canvas.configure(height=200)
    canvas.pack(side="left",fill="both",expand=True)
    scrollbar.pack(side="right",fill="y")
    stratFrame.pack()
    length=len(stocks["values"])
    i=0
    for value in stocks["values"]:
        Label(scrollableFrame,text=value,width=20,font=("Arial Black",10)).grid(row=i,column=0)
        Label(scrollableFrame,text="",width=20,font=("Arial Black",10)).grid(row=i,column=1)
        i+=1
   
    instruments=[] 
    widgets=scrollableFrame.winfo_children()
    size=len(widgets)/2
    i=0

    while(i<size):
        instruments.append("NSE:"+widgets[i*2]["text"])
        i+=1
    quotes=kite.quote(instruments)
    i=0
    rsiValue=0
    limit=0
    while(True):
        global stopStrat
        if(stopStrat==True):
            stopStrat=False
            straButton["state"]="active"
            stratFrame.destroy()
            print("STOPPED")
            break
        while(i<size):
            token=quotes["NSE:"+widgets[i*2]["text"]]["instrument_token"]
            to_date=datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            from_date = ((datetime.today()-timedelta(days=30)).strftime("%Y-%m-%d"))+" 09:15:00"
            interval = "5minute"
            data=(kite.historical_data(token, from_date, to_date, interval))
            #condition to place order
            rsiValue=rsi(data)
            if((rsiValue<30)and(close(data,0)>close(data,-1))):
                placeOrder(widgets[i*2]["text"])
                widgets[i*2+1].destroy()
                widgets[i*2].destroy()
                limit+=1
                if(limit==(int)(orderLimit.get())):
                    break
            else:    
                widgets[i*2+1]["text"]=rsiValue
                if(rsiValue>70):
                    widgets[i*2+1].config(fg="green")
                elif(rsiValue<30):
                    widgets[i*2+1].config(fg="red")
                else:
                    widgets[i*2+1].config(fg="orange")
            i+=1   
        if(limit==(int)(orderLimit.get())):
            straButton["state"]="active"
            stratFrame.destroy()
            break
def rsi(data):
    size=len(data)-1
    gain=0
    loss=0
    i=0
    while(i<14):
        if(data[i+1]["close"]>data[i]["close"]):
            gain+=((data[i+1]["close"]-data[i]["close"])/data[i]["close"])*100
        else:
            loss+=((data[i]["close"]-data[i+1]["close"])/data[i+1]["close"])*100
        i+=1
    gain=gain/14
    loss=loss/14
    rsi=100-(100/(1+(gain/loss)))
    while(i<size):
        if(data[i+1]["close"]>data[i]["close"]):
            gain=(gain*13+((data[i+1]["close"]-data[i]["close"])/data[i]["close"])*100)/14
            loss=(loss*13)/14
        else:
            loss=(loss*13+((data[i]["close"]-data[i+1]["close"])/data[i+1]["close"])*100)/14
            gain=(gain*13)/14
        rsi=100-100/(1+(gain/loss))
        i+=1   
    return (float)("{:.2f}".format(rsi))
def close(data,index):
    return data[len(data)-1+index]["close"]
    
topFrame=Frame(root)
Button(topFrame,text="CONNECT",command=popup,width=20,bg="palegreen",fg="black",font=("Arial Black",10)).grid(row=0,column=0)
username=Label(topFrame,width=20,font=("Arial Black",10))
username.grid(row=0,column=1)
Button(topFrame,text="SL&QUANTITY",width=20,bg="palegreen",fg="black",font=("Arial Black",10)).grid(row=0,column=2)


orderFrame=Frame(root)
Label(orderFrame,text="NAME",width=20,bg="gray10",fg="white",font=("Arial Black",10)).grid(row=0,column=0)
stocks=ttk.Combobox(orderFrame,values=["HDFCBANK"],width=22,font=("Arial Bold",10))
stocks.grid(row=0,column=1)
Label(orderFrame,text="LIMIT",width=20,bg="gray10",fg="white",font=("Arial Black",10)).grid(row=0,column=2)
orderLimit=Entry(orderFrame,font=("Arial Black",10))
orderLimit.grid(row=0,column=3)
Label(orderFrame,text="CAPITAL",width=20,bg="gray10",fg="white",font=("Arial Black",10)).grid(row=1,column=0)
capital=Entry(orderFrame,font=("Arial Black",10))
capital.grid(row=1,column=1)
Label(orderFrame,text="LOSS",width=20,bg="gray10",fg="white",font=("Arial Black",10)).grid(row=1,column=2)
loss=Entry(orderFrame,font=("Arial Black",10))
loss.grid(row=1,column=3)

radio = IntVar()  
R1 = Radiobutton(orderFrame,width=15, text="MARKET",bg="gray10",fg="white",font=("Arial Black",10),selectcolor='green', variable=radio,value=1)
R1.grid(row = 3, column = 0)
R2 = Radiobutton(orderFrame,width=15, text="LIMIT",bg="gray10",fg="white",font=("Arial Black",10), selectcolor='green',variable=radio,value=2)
R2.grid(row = 3, column = 1)

radio2 = IntVar()  
R3 = Radiobutton(orderFrame,width=15, text="BUY",bg="gray10",fg="white",font=("Arial Black",10), selectcolor='green', variable=radio2,value=1)
R3.grid(row = 4, column = 0)
R4 = Radiobutton(orderFrame,width=15, text="SELL",bg="gray10",fg="white",font=("Arial Black",10), selectcolor='green', variable=radio2,value=2)
R4.grid(row = 4, column = 1)
straButton=Button(orderFrame,text="START STRATEGY",width=18,bg="palegreen",fg="black",font=("Arial Black",10),command=lambda:startThread(1))
straButton.grid(row=4,column=2)
Button(orderFrame,text="STOP STRATEGY",width=18,bg="palegreen",fg="black",font=("Arial Black",10),command=lambda:stopThread(1)).grid(row=4,column=3)
Button(orderFrame,text="ORDERS",width=18,font=("Arial Black",10),command=position).grid(row=5,column=0)
Button(orderFrame,text="POSITION",width=18,font=("Arial Black",10),command=lambda:startThread(0)).grid(row=5,column=1)
Button(orderFrame,text="STOP",width=18,font=("Arial Black",10),command=lambda:stopThread(0)).grid(row=5,column=2)

profitLabel=Label(orderFrame,text="",width=15,font=("Arial Black",12))
profitLabel.grid(row=5,column=3)
positionFrame=Frame(root)



topFrame.config(background="black")
orderFrame.config(background="gray10")
positionFrame.config(background="gray20")

topFrame.pack()
orderFrame.pack()
positionFrame.pack()

root.mainloop()
