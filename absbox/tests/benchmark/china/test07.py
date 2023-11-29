from absbox.local.china import SPV

BYD_AUTO_2021_2 = SPV(
    "盛世融迪2021年第二期个人汽车抵押贷款"
    ,{"封包日":"2022-07-01","起息日":"2022-07-26","首次兑付日":"2022-08-26"
     ,"法定到期日":"2060-12-01","收款频率":"月初","付款频率":["每月",26]}     
    ,{'发行':{'资产池规模':2_353_348_391.45},
      '归集表':[
         ["2022-07-01",1234640402,78501963.04,954043.04]
        ,["2022-08-01",1157265240,77375162.14,891648.43]
        ,["2022-09-01",1080039426,77225814.18,852731.81]
        ,["2022-10-01",1003282127,76757298.83,813484.98]
        ,["2022-11-01",927427822.3,75854304.71,774101.65]
        ,["2022-12-01",852342434.9,75085387.41,734796.34]
        ,["2023-01-01",780421405.7,71921029.21,695343.12]
        ,["2023-02-01",712379940.6,68041465.05,655964.23]
        ,["2023-03-01",646490512.9,65889427.7,616857.95]
        ,["2023-04-01",584776933.2,61713579.7,577837.7]
        ,["2023-05-01",527867450.6,56909482.63,539331.19]
        ,["2023-06-01",470936840,56930610.58,503085.91]
        ,["2023-07-01",414031471.4,56905368.58,466755.77]
        ,["2023-08-01",357238793.3,56792678.1,430516.11]
        ,["2023-09-01",300569969.4,56668823.93,394715.61]
        ,["2023-10-01",244168401.8,56401567.55,359341.72]
        ,["2023-11-01",188261231.6,55907170.23,324663.69]
        ,["2023-12-01",135214312.8,53046918.78,290675.76]
        ,["2024-01-01",95533450.08,39680862.74,257752.51]
        ,["2024-02-01",68181138.8,27352311.28,226897.8]
        ,["2024-03-01",47481932.22,20699206.58,198247.82]
        ,["2024-04-01",36412499.53,11069432.69,170618.51]
        ,["2024-05-01",31649692,4762807.53,145060.5]
        ,["2024-06-01",26867972.61,4781719.39,126148.64]
        ,["2024-07-01",22080740.39,4787232.22,107159.95]
        ,["2024-08-01",17274503.87,4806236.52,88155.65]
        ,["2024-09-01",12570553.22,4703950.65,69099.78]
        ,["2024-10-01",8474147.69,4096405.53,50432.44]
        ,["2024-11-01",5267663.01,3206484.68,34157.1]
        ,["2024-12-01",3048007.94,2219655.07,21390.56]
        ,["2025-01-01",1782441.89,1265566.05,12491.46]
        ,["2025-02-01",972018.42,810423.47,7372.76]
        ,["2025-03-01",353663.75,618354.67,4075.94]
        ,["2025-04-01",0,353663.75,1537.85]
      ]
     }
    ,(("本金分账户",{"余额":0 })
      ,("收入分账户",{"余额":0})
      ,("流动性储备",{"余额":15_757_708.38
                    ,"类型":{"较高":[{"目标储备金额":["资产池余额",0.012]}
                                   ,{"目标储备金额":["初始资产池余额",0.005]}]}})
      ,("收款账户",{"余额":0 })
     )
    ,(("优先",{"当前余额":716_739_000.00
             ,"当前利率":0.026
             ,"初始余额":1_730_000_000.00
             ,"初始利率":0.026
             ,"起息日":"2022-07-26"
             ,"利率":{"固定":0.026}
             ,"债券类型":{"过手摊还":None}
             })
      ,("次级",{"当前余额":270_000_000.00
             ,"当前利率":0.0
             ,"初始余额":270_000_000.00
             ,"初始利率":0.0
             ,"起息日":"2022-07-26"
             ,"利率":{"固定":0.00}
             ,"债券类型":{"权益":None}
             }))
    ,(("增值税",{"类型":{"百分比费率":["资产池当期","利息",0.0326]}})
      ,("服务商费用",{"类型":{"年化费率":["资产池余额",0.0012]}})
     )
    ,{"未违约":[
         ["账户转移","流动性储备","收入分账户"]
         ,["支付费用","收入分账户",["服务商费用"]]
         ,["计提支付利息","收入分账户",["优先"]]
         ,["账户转移","收入分账户","流动性储备",{"储备":"缺口"}]
         #,["按公式账户转移","收入分账户","本金分账户","A+B+C-D"]
         ,["账户转移","收入分账户","本金分账户"]
         ,["支付本金","本金分账户",["优先"]]
         ,["支付本金","本金分账户",["次级"]]
         ,["支付收益","本金分账户","次级"]
      ]
     ,"回款后":[["计提费用","增值税"]]
     ,"清仓回购":[
                 ["出售资产",["正常|违约",1.0,0.0],"本金分账户"]
                 ,["账户转移","收入分账户","收款账户"]
                 ,["账户转移","本金分账户","收款账户"]
                 ,["账户转移","流动性储备","收款账户"]
                 ,["支付费用","收款账户",["服务商费用"]]
                ,["计提支付利息","收款账户",["优先"]]
                ,["支付本金","收款账户",["优先"]]
                ,["支付本金","收款账户",["次级"]]
                ,["支付收益","收款账户","次级"]
             ]
     }
    ,(["利息回款","收入分账户"]
      ,["本金回款","本金分账户"]
      ,["早偿回款","本金分账户"]
      ,["回收回款","本金分账户"])
    ,None
    ,None
    ,None
    ,None
    ,("设计","摊销")
)
