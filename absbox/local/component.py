from absbox.local.util import mkTag, DC, mkTs, guess_locale, readTagStr, subMap, subMap2, renameKs, ensure100
from absbox.local.util import mapListValBy, uplift_m_list, mapValsBy, allList, getValWithKs, applyFnToKey
from absbox.local.util import earlyReturnNone, mkFloatTs, mkRateTs, mkRatioTs

from absbox.local.base import *
from enum import Enum
import itertools
import functools
import logging
from typing import Union

import pandas as pd
#from pyspecter import query, S

def mkLiq(x):
    ''' make pricing method '''
    if x == {"正常余额折价": cf, "违约余额折价": df}:
        return mkTag(("BalanceFactor", [cf, df]))
    elif x == {"CurrentFactor": cf, "DefaultFactor": df}:
        return mkTag(("BalanceFactor", [cf, df]))
    elif x == {"贴现计价": df, "违约余额回收率": r}:
        return mkTag(("PV", [df, r]))
    elif x == {"PV": df, "DefaultRecovery": r}:
        return mkTag(("PV", [df, r]))
    else:
        raise RuntimeError(f"Failed to match {x} in Liquidation Method")

def mkDatePattern(x):
    ''' make date pattern '''
    if len(x) == 2 and x[0] == "每月":
        return mkTag((datePattern["每月"], x[1]))
    elif len(x) == 3 and x[0] == "每年":
        return mkTag((datePattern["每年"], [x[1], x[2]]))
    elif len(x) == 2 and x[0] == "DayOfMonth":
        return mkTag(("DayOfMonth", x[1]))
    elif len(x) == 3 and x[0] == "MonthDayOfYear":
        return mkTag(("MonthDayOfYear", x[1], x[2]))
    elif len(x) >= 1 and x[0] == "CustomDate":
        return mkTag(("CustomDate", x[1:]))
    elif len(x) == 3 and x[0] == "EveryNMonth":
        return mkTag(("EveryNMonth", [x[1], x[2]]))
    elif len(x) >= 1 and x[0] == "AllDatePattern":
        return mkTag(("AllDatePattern", [mkDatePattern(_) for _ in x[1:]]))
    elif len(x) == 3 and (x[0] == "After" or x[0] == "之后"):
        return mkTag(("StartsExclusive", [x[1], mkDatePattern(x[2])]))
    elif len(x) == 3 and (x[0] == "ExcludeDatePattern" or x[0] == "排除"):
        return mkTag(("Exclude", [mkDatePattern(x[1]), [mkDatePattern(_) for _ in x[2]]]))
    elif len(x) == 3 and (x[0] == "OffsetDateDattern" or x[0] == "平移"):
        return mkTag(("OffsetBy", [mkDatePattern(x[1]), x[2]]))
    elif x in datePattern.values():
        return mkTag((x,))
    elif x in datePattern.keys():
        return mkTag((datePattern[x],))
    else:
        raise RuntimeError(f"Failed to match {x}")



def getStartDate(x):
    if isinstance(x, dict):
        if set(x.keys()) == {"封包日", "起息日", "首次兑付日", "法定到期日", "收款频率", "付款频率"} \
                or set(x.keys()) == {"cutoff", "closing", "firstPay", "stated", "poolFreq", "payFreq"}:
            return (x["封包日"] if "封包日" in x else x["cutoff"], x["起息日"] if "起息日" in x else x["closing"])

        elif set(x.keys()) == {"归集日", "兑付日", "法定到期日", "收款频率", "付款频率"} \
                or set(x.keys()) == {"collect", "pay", "stated", "poolFreq", "payFreq"}:
            return (x["归集日"][0] if "归集日" in x else x["collect"][0], x["兑付日"][0] if "兑付日" in x else x["pay"][0])

def mkDate(x):
    ''' make date component for deal '''
    if ("封包日" in x or "起息日" in x or "首次兑付日" in x or "法定到期日" in x or "收款频率" in x or "付款频率" in x) or \
        ("cutoff" in x or "closing" in x or "firstPay" in x or "stated" in x or "poolFreq" in x or "payFreq" in x):
        a = x.get("封包日", x.get("cutoff", None))
        b = x.get("起息日", x.get("closing", None))
        c = x.get("首次兑付日", x.get("firstPay", None))
        d = x.get("法定到期日", x.get("stated", None))
        pf = x.get("收款频率", x.get("poolFreq", None))
        bf = x.get("付款频率", x.get("payFreq", None))
        firstCollection = x.get("首次归集日", b)
        mr = x.get("循环结束日", None)

        return mkTag(("PreClosingDates", [a, b, mr, d, [firstCollection, mkDatePattern(pf)], [c, mkDatePattern(bf)]]))

    elif ("归集日" in x or "兑付日" in x or "法定到期日" in x or "收款频率" in x or "付款频率" in x) or \
        ("collect" in x or "pay" in x or "stated" in x or "poolFreq" in x or "payFreq" in x):
        lastCollected, nextCollect = x.get("归集日", x.get("collect", (None, None)))
        pp, np = x.get("兑付日", x.get("pay", (None, None)))
        c = x.get("法定到期日", x.get("stated", None))
        pf = x.get("收款频率", x.get("poolFreq", None))
        bf = x.get("付款频率", x.get("payFreq", None))

        mr = x.get("循环结束日", None)

        return mkTag(("CurrentDates", [[lastCollected, pp],
                                       mr,
                                       c,
                                       [nextCollect, mkDatePattern(pf)],
                                       [np, mkDatePattern(bf)]]))

    elif ("回款日" in x or "分配日" in x or "封包日" in x or "起息日" in x) or \
        ("poolCollection" in x or "distirbution" in x or "cutoff" in x or "closing" in x):
        cdays = x.get("回款日", x.get("poolCollection", None))
        ddays = x.get("分配日", x.get("distirbution", None))
        cutoffDate = x.get("封包日", x.get("cutoff", None))
        closingDate = x.get("起息日", x.get("closing", None))

        return mkTag(("CustomDates", [cutoffDate, [mkTag(("PoolCollection", [cd, ""])) for cd in cdays], closingDate, [mkTag(("RunWaterfall", [dd, ""])) for dd in ddays]]))

    else:
        raise RuntimeError(f"Failed to match: {x} in Dates")

def mkDsRate(x):
    if isinstance(x,float):
        return mkDs(("constant",x))
    else:
        return mkDs(x)

def mkFeeType(x):
    if "年化费率" in x or "annualPctFee" in x:
        base, rate = x.get("年化费率", x.get("annualPctFee"))
        return mkTag(("AnnualRateFee", [mkDs(base), mkDsRate(rate)]))
    elif "百分比费率" in x or "pctFee" in x:
        desc, _rate = x.get("百分比费率", x.get("pctFee", [])), x.get(_rate)
        rate = mkDsRate(_rate)
        if desc == ["资产池当期", "利息"] or desc == ["poolCurrentCollection", "interest"] or desc == ["资产池回款", "利息"]:
            return mkTag(("PctFee", [mkTag(("PoolCurCollection", ["CollectedInterest"])), rate]))
        elif desc == ["已付利息合计", *bns] or desc == ["paidInterest", *bns]:
            return mkTag(("PctFee", [mkTag(("LastBondIntPaid", bns)), rate]))
        elif desc == ["已付本金合计", *bns] or desc == ["paidPrincipal", *bns]:
            return mkTag(("PctFee", [mkTag(("LastBondPrinPaid", bns)), rate]))
        else:
            raise RuntimeError(f"Failed to match on 百分比费率：{desc, rate}")
    elif "固定费用" in x or "fixFee" in x:
        amt = x.get("固定费用", x.get("fixFee"))
        return mkTag(("FixFee", amt))
    elif "周期费用" in x or "recurFee" in x:
        p, amt = x.get("周期费用", x.get("recurFee", []))
        return mkTag(("RecurFee", [mkDatePattern(p), amt]))
    elif "自定义" in x or "customFee" in x:
        fflow = x.get("自定义", x.get("customFee"))
        return mkTag(("FeeFlow", mkTs("BalanceCurve", fflow)))
    elif "计数费用" in x or "numFee" in x:
        p, s, amt = x.get("计数费用", x.get("numFee", []))
        return mkTag(("NumFee", [mkDatePattern(p), mkDs(s), amt]))
    elif "差额费用" in x or "targetBalanceFee" in x:
        ds1, ds2 = x.get("差额费用", x.get("targetBalanceFee", []))
        return mkTag(("TargetBalanceFee", [mkDs(ds1), mkDs(ds2)]))
    else:
        raise RuntimeError(f"Failed to match on fee type: {x}")


def mkDateVector(x):
    if x == dp: 
        if isinstance(dp, str):
            return mkTag(datePattern[dp])
    elif x == [dp, *p]: 
        if (dp in datePattern.keys()):
            return mkTag((datePattern[dp], p))
    else:
        raise RuntimeError(f"not match found: {x}")

def mkPoolSource(x):
    if x in ["利息", "Interest", "利息回款", "CollectedInterest"]:
        return "CollectedInterest"
    elif x in ["本金", "Principal", "本金回款", "CollectedPrincipal"]:
        return "CollectedPrincipal"
    elif x in ["回收", "Recovery", "回收回款", "CollectedRecoveries"]:
        return "CollectedRecoveries"
    elif x in ["早偿", "Prepayment", "早偿回款", "CollectedPrepayment"]:
        return "CollectedPrepayment"
    elif x in ["租金", "Rental", "租金回款", "CollectedRental"]:
        return "CollectedRental"
    elif x in ["现金", "Cash", "现金回款", "CollectedCash"]:
        return "CollectedCash"
    elif x in ["新增违约", "Defaults"]:
        return "NewDefaults"
    elif x in ["新增拖欠", "Delinquencies"]:
        return "NewDelinquencies"
    elif x in ["新增损失", "Losses"]:
        return "NewLosses"
    else:
        raise RuntimeError(f"no match found: {x} :make Pool Source")


@functools.lru_cache(maxsize=128)
def mkDs(x):
    "Making Deal Stats"
    if x == ("债券余额",) | ("bondBalance",):
        return mkTag("CurrentBondBalance")
    elif x == ("债券余额", *bnds) | ("bondBalance", *bnds):
        return mkTag(("CurrentBondBalanceOf", bnds))
    elif x == ("初始债券余额",) | ("originalBondBalance",):
        return mkTag("OriginalBondBalance")
    elif x == ("到期月份", bn) | ("monthsTillMaturity", bn):
        return mkTag(("MonthsTillMaturity", bn))
    elif x == ("资产池余额",) | ("poolBalance",):
        return mkTag("CurrentPoolBalance")
    elif x == ("资产池期初余额",) | ("poolBegBalance",):
        return mkTag("CurrentPoolBegBalance")
    elif x == ("初始资产池余额",) | ("originalPoolBalance",):
        return mkTag("OriginalPoolBalance")
    elif x == ("资产池违约余额",) | ("currentPoolDefaultedBalance",):
        return mkTag("CurrentPoolDefaultedBalance")
    elif x == ("资产池累计损失余额",) | ("cumPoolNetLoss",):
        return mkTag("CumulativeNetLoss")
    elif x == ("资产池累计损失率",) | ("cumPoolNetLossRate",):
        return mkTag("CumulativeNetLossRatio")
    elif x == ("资产池累计违约余额",) | ("cumPoolDefaultedBalance",):
        return mkTag("CumulativePoolDefaultedBalance")
    elif x == ("资产池累计回收额",) | ("cumPoolRecoveries",):
        return mkTag("CumulativePoolRecoveriesBalance")
    elif x == ("资产池累计违约率",) | ("cumPoolDefaultedRate",):
        return mkTag("CumulativePoolDefaultedRate")
    elif x == ("资产池累计违约率",n) | ("cumPoolDefaultedRate",n):
        return mkTag(("CumulativePoolDefaultedRateTill",n))
    elif x == ("资产池累计", *i) | ("cumPoolCollection", *i):
        return mkTag(("PoolCumCollection", [mkPoolSource(_) for _ in i]))
    elif x == ("资产池累计至", idx, *i) | ("cumPoolCollectionTill", idx, *i):
        return mkTag(("PoolCumCollectionTill", [idx, [mkPoolSource(_) for _ in i]] ))
    elif x == ("资产池当期", *i) | ("curPoolCollection", *i):
        return mkTag(("PoolCurCollection", [mkPoolSource(_) for _ in i]))
    elif x == ("资产池当期至", idx, *i) | ("curPoolCollectionStats", idx, *i):
        return mkTag(("PoolCurCollectionStats", [idx, [mkPoolSource(_) for _ in i]]))
    elif x == ("债券系数",) | ("bondFactor",):
        return mkTag("BondFactor")
    elif x == ("资产池系数",) | ("poolFactor",):
        return mkTag("PoolFactor")
    elif x == ("债券利率", bn) | ("bondRate", bn):
        return mkTag(("BondRate", bn))
    elif x == ("债券加权利率", *bn) | ("bondWaRate", *bn):
        return mkTag(("BondWaRate", bn))
    elif x == ("资产池利率",) | ("poolWaRate",):
        return mkTag("PoolWaRate")
    elif x == ("所有账户余额",) | ("accountBalance"):
        return mkTag("AllAccBalance")
    elif x == ("账户余额", *ans) | ("accountBalance", *ans):
        return mkTag(("AccBalance", ans))
    elif x == ("账簿余额", *ans) | ("ledgerBalance", *ans):
        return mkTag(("LedgerBalance", ans))
    elif x == ("账簿发生额", lns, cmt) | ("ledgerTxnAmount", lns, cmt):
        return mkTag(("LedgerTxnAmt", [lns, mkComment(cmt)]))
    elif x == ("账簿发生额", lns) | ("ledgerTxnAmount", lns):
        return mkTag(("LedgerTxnAmt", [lns,None]))
    elif x == ("债券待付利息", *bnds) | ("bondDueInt", *bnds):
        return mkTag(("CurrentDueBondInt", bnds))
    elif x == ("债券已付利息", *bnds) | ("lastBondIntPaid", *bnds):
        return mkTag(("LastBondIntPaid", bnds))
    elif x == ("债券低于目标余额", bn) | ("behindTargetBalance", bn):
        return mkTag(("BondBalanceGap", bn))
    elif x == ("已提供流动性", *liqName) | ("liqBalance", *liqName):
        return mkTag(("LiqBalance", liqName))
    elif x == ("流动性额度", *liqName) | ("liqCredit", *liqName):
        return mkTag(("LiqCredit", liqName))
    elif x == ("rateCapNet",n):
        return mkTag(("RateCapNet", n))
    elif x == ("rateSwapNet",n):
        return mkTag(("RateSwapNet", n))
    elif x == ("债务人数量",) | ("borrowerNumber",):
        return mkTag(("CurrentPoolBorrowerNum"))
    elif x == ("事件", loc, idx) | ("trigger", loc, idx):
        if not loc in dealCycleMap:
            raise RuntimeError(f" {loc} not in map {dealCycleMap}")
        return mkTag(("TriggersStatus", [dealCycleMap[loc], idx]))
    elif x == ("阶段", st) | ("status", st):
        return mkTag(("IsDealStatus", mkStatus(st)))
    elif x == ("待付费用", *fns) | ("feeDue", *fns):
        return mkTag(("CurrentDueFee", fns))
    elif x == ("已付费用", *fns) | ("lastFeePaid", *fns):
        return mkTag(("LastFeePaid", fns))
    elif x == ("费用支付总额", cmt, *fns) | ("feeTxnAmount", cmt, *fns):
        return mkTag(("FeeTxnAmt", [fns, cmt]))
    elif x == ("债券支付总额", cmt, *bns) | ("bondTxnAmount", cmt, *bns):
        return mkTag(("BondTxnAmt", [bns, cmt]))
    elif x == ("账户变动总额", cmt, *ans) | ("accountTxnAmount", cmt, *ans):
        return mkTag(("AccTxnAmt", [ans, cmt]))
    elif x == ("系数", ds, f) | ("factor", ds, f) | ("*", ds, f) :
        if isinstance(f, float):
            return mkTag(("Factor", [mkDs(ds), f]))
    elif x == ("Min", *ds) | ("min", *ds):
        return mkTag(("Min", [mkDs(s) for s in ds]))
    elif x == ("Max", *ds) | ("max", *ds):
        return mkTag(("Max", [mkDs(s) for s in ds]))
    elif x == ("合计", *ds) | ("sum", *ds) | ("+", *ds):
        return mkTag(("Sum", [mkDs(_ds) for _ds in ds]))
    elif x == ("差额", *ds) | ("substract", *ds) | ("subtract", *ds) | ("-", *ds):
        return mkTag(("Substract", [mkDs(_ds) for _ds in ds]))
    elif x == ("常数", n) | ("constant", n) | ("const", n):
        return mkTag(("Constant", n))
    elif x == ("储备账户缺口", *accs) | ("reserveGap", *accs):
        return mkTag(("ReserveAccGap", accs))
    elif x == ("储备账户盈余", *accs) | ("reserveExcess", *accs):
        return mkTag(("ReserveExcess", accs))
    elif x == ("最优先", bn, bns) | ("isMostSenior", bn, bns):
        return mkTag(("IsMostSenior", bn, bns))
    elif x == ("清偿完毕", *bns) | ("isPaidOff", *bns):
        return mkTag(("IsPaidOff", bns))
    elif x == ("比率测试", ds, op, r) | ("rateTest", ds, op, r):
        return mkTag(("TestRate", [mkDs(ds), op_map[op], r]))
    elif x == ("所有测试", b, *ds) | ("allTest", b, *ds):
        return mkTag(("TestAll", [b, [mkDs(_) for _ in ds]]))
    elif x == ("任一测试", b, *ds) | ("anyTest", b, *ds):
        return mkTag(("TestAny", [b, [mkDs(_) for _ in ds]]))
    elif x == ("自定义", n) | ("custom", n):
        return mkTag(("UseCustomData", n))
    elif x == ("区间内", floor, cap, s) | ("floorCap", floor, cap, s):
        return mkTag(("FloorAndCap", [floor, cap, s]))
    elif x == ("floorWith", ds1, ds2):
        return mkTag(("FloorWith", [mkDs(ds1), mkDs(ds2)]))
    elif x == ("floorWithZero", ds1):
        return mkTag(("FloorWithZero", mkDs(ds1)))
    elif x == ("capWith", ds1, ds2):
        return mkTag(("CapWith", [mkDs(ds1), mkDs(ds2)]))
    elif x == ("/", ds1, ds2) | ("divide", ds1, ds2):
        return mkTag(("Divide", [mkDs(ds1), mkDs(ds2)]))
    elif x == ("abs", ds):
        return mkTag(("Abs", mkDs(ds)))
    elif x == ("avg", *ds) | ("平均", *ds):
        return mkTag(("Avg", [mkDs(_) for _ in ds]))
    else:
        raise RuntimeError(f"Failed to match DS/Formula: {x}")

def mkCurve(tag,xs):
    return mkTag((tag,xs))

def mkPre(p):
    def queryType(y):
        if y == (_y,*_): 
            if _y in rateLikeFormula:
                return "IfRate"
        elif y == ("avg",*ds): 
            if set([_[0] for _ in ds]).issubset(rateLikeFormula):
                return "IfRate"
        elif y == (_y,*_): 
            if _y in intLikeFormula:
                return "IfInt"
            # case (_y,*_) if _y in boolLikeFormula:
            #     return "IfBool"
        else:
            return "If"
            
    if p == ["状态", _st] | ["status", _st]:
        return mkTag(("IfDealStatus", mkStatus(_st)))
    elif p == ["同时满足", *_p] | ["all", *_p]:
        return mkTag(("All", [mkPre(p) for p in _p]))
    elif p == ["任一满足", *_p] | ["any", *_p]:
        return mkTag(("Any", [mkPre(p) for p in _p]))
    elif p == [ds, "=", 0]:
        return mkTag(("IfZero", mkDs(ds)))
    elif p == [ds, b] | [ds, b]: 
        if isinstance(b, bool):
            return mkTag(("IfBool", [mkDs(ds), b]))
    elif p == [ds1, op, ds2]:
        if (isinstance(ds1, tuple) and isinstance(ds2, tuple)):
            q = queryType(ds1)
            return mkTag((f"{q}2", [op_map[op], mkDs(ds1), mkDs(ds2)]))
    elif p == [ds, op, curve]:
        if isinstance(curve, list):
            q = queryType(ds)
            return mkTag((f"{q}Curve", [op_map[op], mkDs(ds), mkCurve("ThresholdCurve", curve)]))
    elif p == [ds, op, n]:
        q = queryType(ds)
        return mkTag((q, [op_map[op], mkDs(ds), n]))
    elif p == [op, _d]:
        return mkTag(("IfDate", [op_map[op], _d]))
    else:
        raise RuntimeError(f"Failed to match on Pre: {p}")


def mkAccInt(x):
    if isinstance(x, dict):
        if "周期" in x and "利率" in x and "利差" in x and "最近结息日" in x:
            return mkTag(("InvestmentAccount", [x["利率"], x["利差"], x["最近结息日"], mkDatePattern(x["周期"])]))
        elif "周期" in x and "利率" in x and "最近结息日" in x:
            return mkTag(("BankAccount", [x["利率"], x["最近结息日"], mkDatePattern(x["周期"])]))
        elif x is None:
            return None
        raise RuntimeError(f"Failed to match on Pre: {p}")


def mkAccType(x):
    if isinstance(x, tuple):
        if x[0] in ("固定", "fix") or x == {"固定储备金额": amt} or x == {"fixReserve": amt}:
            return mkTag(("FixReserve", x[1]))
        elif x[0] in ("目标", "target") or x == {"目标储备金额": [base, rate]} or x == {"targetReserve": [base, rate]}:
            if isinstance(base, tuple) and base[0] in ("合计", "sum") or base == ("合计", *qs) or base == ["Sum", *qs]:
                sumDs = [mkDs(q) for q in qs]
                return mkTag(("PctReserve", [mkTag(("Sum", sumDs)), rate]))
            else:
                return mkTag(("PctReserve", [mkDs(base), rate]))
        elif x == {"目标储备金额": {"公式": ds, "系数": rate}} or x == {"targetReserve": {"formula": ds, "factor": rate}}:
            return mkTag(("PctReserve", [mkDs(ds), rate]))
        elif x[0] in ("目标", "target") and len(x) == 2:
            return mkTag(("PctReserve", [mkDs(x[1]), 1.0]))
        else:
            raise RuntimeError(f"Failed to match {x} for account reserve type")
    elif isinstance(x, dict):
        if "较高" in x or "max" in x and isinstance(x["较高" if "较高" in x else "max"], list):
            return mkTag(("Max", [mkAccType(_) for _ in x["较高" if "较高" in x else "max"]]))
        elif ("较高", *_s) in x or ("max", *_s) in x:
            return mkTag(("Max", [mkAccType(_) for _ in _s]))
        elif "较低" in x or "min" in x and isinstance(x["较低" if "较低" in x else "min"], list):
            return mkTag(("Min", [mkAccType(_) for _ in x["较低" if "较低" in x else "min"]]))
        elif ("较低", *_s) in x or ("min", *_s) in x:
            return mkTag(("Min", [mkAccType(_) for _ in _s]))
        elif "分段" in x or "When" in x:
            p, a, b = x.get("分段" if "分段" in x else "When", (None, None, None))
            return mkTag(("Either", [mkPre(p), mkAccType(a), mkAccType(b)]))
        else:
            raise RuntimeError(f"Failed to match {x} for account reserve type")




def mkAccTxn(xs):
    "AccTxn T.Day Balance Amount Comment"
    if xs is None:
        return None
    else:
        return [mkTag(("AccTxn", x)) for x in xs]


def mkAcc(an, x):
    if "余额" in x or "balance" in x:
        b = x.get("余额", x.get("balance"))
        t = x.get("类型", x.get("type"))
        i = x.get("计息", x.get("interest"))
        tx = x.get("记录", x.get("txn"))
        return {"accBalance": b, "accName": an, "accType": mkAccType(t), "accInterest": mkAccInt(i), "accStmt": mkAccTxn(tx)}
    else:
        raise RuntimeError(f"Failed to match account: {an}, {x}")


def mkBondType(x):
    if isinstance(x, dict):
        if set(x.keys()) <= {"固定摊还", "PAC"}:
            return mkTag(("PAC", mkTag(("BalanceCurve", x.get("固定摊还", x.get("PAC"))))))
        elif set(x.keys()) <= {"过手摊还", "Sequential"}:
            return mkTag(("Sequential",))
        elif set(x.keys()) <= {"锁定摊还", "Lockout"}:
            return mkTag(("Lockout", x.get("锁定摊还", x.get("Lockout"))))
        elif set(x.keys()) <= {"权益", "Equity"}:
            return mkTag(("Equity",))
    else:
        raise RuntimeError(f"Failed to match bond type: {x}")


def mkBondRate(x):
    if isinstance(x, dict):
        if set(x.keys()) <= {"浮动", "日历", "floater", "dayCount"}:
            return mkTag(("Floater", [
                x.get("浮动", x.get("floater"))[0],
                x.get("浮动", x.get("floater"))[1],
                x.get("浮动", x.get("floater"))[2],
                mkDatePattern(x.get("浮动", x.get("floater"))[3]),
                x.get("日历", x.get("dayCount")),
                None,
                None
            ]))
        elif set(x.keys()) <= {"浮动", "floater"}:
            return mkBondRate(x | {"日历": DC.DC_ACT_365F.value, "dayCount": DC.DC_ACT_365F.value})
        elif set(x.keys()) <= {"固定", "日历", "fix", "dayCount"}:
            return mkTag(("Fix", [x.get("固定", x.get("fix")), x.get("日历", x.get("dayCount"))]))
        elif set(x.keys()) <= {"固定", "Fixed", "fix"}:
            return mkTag(("Fix", [x.get("固定", x.get("Fixed")), DC.DC_ACT_365F.value]))
        elif set(x.keys()) == {"期间收益"}:
            return mkTag(("InterestByYield", x["期间收益"]))
        elif set(x.keys()) <= {"上限", "cap", "下限", "floor"}:
            rate_type = "CapRate" if "上限" in x or "cap" in x else "FloorRate"
            return mkTag((rate_type, [mkBondRate(x[rate_type][1]), x[rate_type][0]]))
    else:
        raise RuntimeError(f"Failed to match bond rate type:{x}")

        
def mkStepUp(x):
    if x == ("ladder",d,spd,dp):
        return mkTag(("PassDateLadderSpread",[d,spd,mkDatePattern(dp)]))
    elif x == ("once",d,spd):
        return mkTag(("PassDateSpread",[d,spd]))
    else:
        raise RuntimeError(f"Failed to match bond step up type:{x}")


def mkBnd(bn, x):
    md = getValWithKs(x, ["到期日", "maturityDate"])
    lastAccrueDate = getValWithKs(x, ["计提日", "lastAccrueDate"])
    lastIntPayDate = getValWithKs(x, ["付息日", "lastIntPayDate"])
    dueInt = getValWithKs(x, ["应付利息", "dueInt"], defaultReturn=0)
    mSt = earlyReturnNone(mkStepUp, getValWithKs(x, ["调息", "stepUp"], defaultReturn=None))
    
    if isinstance(x, dict) and set(x.keys()) <= {"当前余额", "当前利率", "初始余额", "初始利率", "起息日", "利率", "债券类型",
                                                  "balance", "rate", "originBalance", "originRate", "startDate",
                                                  "rateType", "bondType"}:
        return {"bndName": bn, "bndBalance": x.get("当前余额", x.get("balance")),
                "bndRate": x.get("当前利率", x.get("rate")),
                "bndOriginInfo": {"originBalance": x.get("初始余额", x.get("originBalance")),
                                  "originDate": x.get("起息日", x.get("startDate")),
                                  "originRate": x.get("初始利率", x.get("originRate"))} | {"maturityDate": md},
                "bndInterestInfo": mkBondRate(x.get("利率", x.get("rateType"))),
                "bndType": mkBondType(x.get("债券类型", x.get("bondType"))),
                "bndDuePrin": 0, "bndDueInt": dueInt, "bndDueIntDate": lastAccrueDate,
                "bndStepUp": mSt, "bndLastIntPayDate": lastIntPayDate
        }
    
    elif isinstance(x, dict) and set(x.keys()) <= {"初始余额", "初始利率", "起息日", "利率", "债券类型",
                                                    "originBalance", "originRate", "startDate",
                                                    "rateType", "bondType"}:
        return {"bndName": bn, "bndBalance": x.get("初始余额", x.get("originBalance")),
                "bndRate": x.get("初始利率", x.get("originRate")),
                "bndOriginInfo": {"originBalance": x.get("初始余额", x.get("originBalance")),
                                  "originDate": x.get("起息日", x.get("startDate")),
                                  "originRate": x.get("初始利率", x.get("originRate"))} | {"maturityDate": md},
                "bndInterestInfo": mkBondRate(x.get("利率", x.get("rateType"))),
                "bndType": mkBondType(x.get("债券类型", x.get("bondType"))),
                "bndDuePrin": 0, "bndDueInt": dueInt, "bndDueIntDate": lastAccrueDate,
                "bndStepUp": mSt, "bndLastIntPayDate": lastIntPayDate
        }


    else:
        raise RuntimeError(f"Failed to match bond:{bn},{x}:mkBnd")


def mkLiqMethod(x):
    if x == ["正常|违约", a, b] | ["Current|Defaulted", a, b]:
        return mkTag(("BalanceFactor", [a, b]))
    elif x == ["正常|拖欠|违约", a, b, c] | ["Current|Delinquent|Defaulted", a, b, c]:
        return mkTag(("BalanceFactor2", [a, b, c]))
    elif x == ["贴现|违约", a, b] | ["PV|Defaulted", a, b]:
        return mkTag(("PV", [a, b]))
    elif x == ["贴现曲线", ts] | ["PVCurve", ts]:
        return mkTag(("PVCurve", mkTs("PricingCurve", ts)))
    else:
        raise RuntimeError(f"Failed to match {x}:mkLiqMethod")

def mkPDA(x):
    if x == {"公式": ds} | {"formula": ds}:
        return mkTag(("DS", mkDs(ds)))
    else:
        raise RuntimeError(f"Failed to match {x}:mkPDA")


def mkAccountCapType(x):
    if x == {"余额百分比": pct} | {"balPct": pct}:
        return mkTag(("DuePct", pct))
    elif x == {"金额上限": amt} | {"balCapAmt": amt}:
        return mkTag(("DueCapAmt", amt))
    else:
        raise RuntimeError(f"Failed to match {x}:mkAccountCapType")

def mkLimit(x:dict):
    if x == {"余额百分比": pct} | {"balPct": pct}:
        return mkTag(("DuePct", pct))
    elif x == {"金额上限": amt} | {"balCapAmt": amt}:
        return mkTag(("DueCapAmt", amt))
    elif x == {"公式": formula} | {"formula": formula}:
        return mkTag(("DS", mkDs(formula)))
    elif x == {"冲销":an} | {"clearLedger":an}:
        return mkTag(("ClearLedger", an))
    elif x == {"簿记":an} | {"bookLedger":an}:
        return mkTag(("BookLedger", an))
    elif x == {"系数":[limit,factor]} | {"multiple":[limit,factor]}:
        return mkTag(("Multiple", [mkLimit(limit),factor]))
    elif x == {"储备":"缺口"} | {"reserve":"gap"} :
        return mkTag(("TillTarget"))
    elif x == {"储备":"盈余"} | {"reserve":"excess"} :
        return mkTag(("TillSource"))
    elif x == None:
        return None
    else:
        raise RuntimeError(f"Failed to match :{x}:mkLimit")
       
def mkComment(x):
    if x == {"payInt":bns}:
        return mkTag(("PayInt",bns))
    elif x == {"payYield":bn}:
        return mkTag(("PayYield",bn))
    elif x == {"transfer":[a1,a2]}:
        return mkTag(("Transfer",[a1,a2]))
    elif x == {"transfer":[a1,a2,limit]}:
        return mkTag(("TransferBy",[a1,a2,limit]))
    elif x == {"direction":d}:
        return mkTag(("TxnDirection",d))
                # = PayInt [BondName]
                # | PayYield BondName 
                # | PayPrin [BondName] 
                # | PayFee FeeName
                # | SeqPayFee [FeeName] 
                # | PayFeeYield FeeName
                # | Transfer AccName AccName 
                # | TransferBy AccName AccName Limit
                # | PoolInflow PoolSource
                # | LiquidationProceeds
                # | LiquidationSupport String
                # | LiquidationDraw
                # | LiquidationRepay
                # | LiquidationSupportInt Balance Balance
                # | BankInt
                # | Empty 
                # | Tag String
                # | UsingDS DealStats
                # | SwapAccure
                # | SwapInSettle
                # | SwapOutSettle
                # | PurchaseAsset
                # | TxnDirection Direction
                # | TxnComments [TxnComment]


def mkLiqDrawType(x):
    if x == "账户" | "account":
        return "LiqToAcc"
    elif x == "费用" | "fee":
        return "LiqToFee"
    elif x == "债券利息" | "interest":
        return "LiqToBondInt"
    elif x == "债券本金" | "principal":
        return "LiqToBondPrin"
    else:
        raise RuntimeError(f"Failed to match :{x}:Liquidation Draw Type")


def mkLiqRepayType(x):
    if x == "余额" | "bal" | "balance":
        return mkTag(("LiqBal"))
    elif x == "费用" | "premium":
        return mkTag(("LiqPremium"))
    elif x == "利息" | "int" | "interest":
        return mkTag(("LiqInt"))
    else:
        raise RuntimeError(f"Failed to match :{x}:Liquidation Repay Type")


def mkRateSwapType(pr, rr):
    def isFloater(y):
        if isinstance(y, tuple):
            return True
        return False
    if (isFloater(pr), isFloater(rr)) == (True, True):
        return mkTag(("FloatingToFloating", [pr, rr]))
    elif (isFloater(pr), isFloater(rr)) == (False, True):
        return mkTag(("FixedToFloating", [pr, rr]))
    elif (isFloater(pr), isFloater(rr)) == (True, False):
        return mkTag(("FloatingToFixed", [pr, rr]))
    else:
        raise RuntimeError(f"Failed to match :{rr,pr}:Interest Swap Type")


def mkRsBase(x):
    if x == {"fix": bal} | {"fixed": bal} | {"固定": bal}:
        return mkTag(("Fixed", bal))
    elif x == {"formula": ds} | {"公式": ds}:
        return mkTag(("Base", mkDs(ds)))
    elif x == {"schedule": tbl} | {"计划": tbl}:
        return mkTs("Balance", tbl)
    else:
        raise RuntimeError(f"Failed to match :{x}:Interest Swap Base")


def mkRateSwap(x):
    if x == {"settleDates": stl_dates, "pair": pair, "base": base
              ,"start": sd, "balance": bal, **p}:
        return {"rsType": mkRateSwapType(*pair),
                    "rsSettleDates": mkDatePattern(stl_dates),
                    "rsNotional": mkRsBase(base),
                    "rsStartDate": sd,
                    "rsPayingRate": p.get("payRate", 0),
                    "rsReceivingRate": p.get("receiveRate", 0),
                    "rsRefBalance": bal,
                    "rsLastStlDate": p.get("lastSettleDate", None),
                    "rsNetCash": p.get("netcash", 0),
                    "rsStmt": p.get("stmt", None)
                    }
    else:
        raise RuntimeError(f"Failed to match :{x}:Interest Swap")

def mkRateCap(x):
    if x == {"index": index, "strike": strike, "base": base, "start": sd
              , "end": ed, "settleDates": dp, "rate": r, **p}:
        return {"rcIndex": index,
                    "rcStrikeRate": mkTs("IRateCurve", strike),
                    "rcNotional": mkRsBase(base),
                    "rcStartDate": sd,
                    "rcSettleDates": mkDatePattern(dp),
                    "rcEndDate": ed,
                    "rcReceivingRate": r,
                    "rcLastStlDate": p.get("lastSettleDate", None),
                    "rcNetCash": p.get("netcash", 0),
                    "rcStmt": p.get("stmt", None)
                    }
    else:
        raise RuntimeError(f"Failed to match :{x}:Interest Cap")



def mkRateType(x):
    if x == {"fix":r} | {"固定":r} | ["fix",r] | ["固定",r]:
        return mkTag(("Fix",[DC.DC_ACT_365F.value, r]))
    elif x == {"floater":(idx,spd),"rate":r,"reset":dp,**p} | \
        {"浮动":(idx,spd),"利率":r,"重置":dp,**p}:
        mf = getValWithKs(p,["floor"])
        mc = getValWithKs(p,["cap"])
        mrnd = getValWithKs(p,["rounding"])
        dc = p.get("dayCount",DC.DC_ACT_365F.value)
        return mkTag(("Floater",[dc,idx,spd,r,mkDatePattern(dp),mf,mc,mrnd]))
    elif x == ["浮动",r,{"基准":idx,"利差":spd,"重置频率":dp,**p}] | \
        ["floater",r,{"index":idx,"spread":spd,"reset":dp,**p}] :
        mf = getValWithKs(p,["floor"])
        mc = getValWithKs(p,["cap"])
        mrnd = getValWithKs(p,["rounding"])
        dc = p.get("dayCount",DC.DC_ACT_365F.value)
        return mkTag(("Floater",[dc,idx,spd,r,mkDatePattern(dp),mf,mc,mrnd]))
    elif x == None:
        return None
    else:
        raise RuntimeError(f"Failed to match :{x}: Rate Type")


def mkBookType(x:list):
    if x == ["PDL", defaults, ledgers] | ["pdl", defaults, ledgers]:
        return mkTag(("PDL",[mkDs(defaults)
                                 ,[[ln, mkDs(ds)] 
                                   for ln, ds in ledgers]]))
    elif x == ["AccountDraw", ledger] | ['accountDraw', ledger]:
        return mkTag(("ByAccountDraw", ledger))
    elif x == ["ByFormula", ledger, dr, ds] | ['formula', ledger, dr, ds]:
        return mkTag(("ByDS", [ledger, dr, mkDs(ds)]))
    else:
        raise RuntimeError(f"Failed to match :{x}:mkBookType")

def mkSupport(x:list):
    if x == ["account", accName, mBookType] | ["suppportAccount", accName, mBookType] | ["支持账户", accName, mBookType]:
        return mkTag(("SupportAccount",[accName, mkBookType(mBookType)]))
    elif x == ["account", accName] | ["suppportAccount", accName] | ["支持账户", accName]:
        return mkTag(("SupportAccount",[accName, None]))
    elif x == ["facility", liqName] | ["suppportFacility", liqName] | ["支持机构", liqName]:
        return mkTag(("SupportLiqFacility", liqName))
    elif x == ["support", *supports] | ["multiSupport", *supports] | ["多重支持", *supports]:
        return mkTag(("MultiSupport", [mkSupport(s) for s in supports]))
    elif x == ["withCondition", pre, s] | ["条件支持", pre, s]:
        return mkTag(("WithCondition", [mkPre(pre), mkSupport(s)]))
    elif x == None:
        return None
    else:
        raise RuntimeError(f"Failed to match :{x}:SupportType")

def mkAction(x: list):
    ''' make waterfall actions '''

    if (x[0] in ["账户转移"] or x[0] in ["transfer"]) and len(x) == 4:
        source = x[1]
        target = x[2]
        m = x[3]
        return mkTag(("Transfer", [mkLimit(m), source, target, None]))
    elif (x[0] in ["账户转移"] or x[0] in ["transfer"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("Transfer", [None, source, target, None]))
    elif (x[0] in ["簿记"] or x[0] in ["bookBy"]) and len(x) == 2:
        bookType = x[1]
        return mkTag(("BookBy", mkBookType(bookType)))
    elif (x[0] in ["计提费用"] or x[0] in ["calcFee"]) and len(x) == 2:
        feenames = [x[1]]
        return mkTag(("CalcFee", feeNames))
    elif (x[0] in ["计提利息"] or x[0] in ["calcInt"]) and len(x) == 2:
        bndNames = [x[1]]
        return mkTag(("CalcBondInt", bndNames))
    elif (x[0] in ["计提支付费用"] or x[0] in ["calcAndPayFee"]) and len(x) == 4:
        source = x[1]
        target = x[2]
        m = x[3]
        limit = getValWithKs(m, ['limit', "限制"])
        support = getValWithKs(m, ['support', "支持"])
        return mkTag(("CalcAndPayFee", [mkLimit(limit), source, target, mkSupport(support)]))
    elif (x[0] in ["计提支付费用"] or x[0] in ["calcAndPayFee"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("CalcAndPayFee",[None, source, target, None]))
    elif (x[0] == "payFee" or x[0] == "支付费用") and len(x) == 4:
        source = x[1]
        target = x[2]
        m = x[3]
        limit = getValWithKs(m, ['limit', "限制"])
        support = getValWithKs(m, ['support', "支持"])
        return mkTag(("PayFee", [mkLimit(limit), source, target, mkSupport(support)]))
    elif (x[0] == "支付费用" or x[0] == "payFee") and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("PayFee", [None, source, target, None]))
    elif (x[0] in ["支付费用收益"] or x[0] in ["payFeeResidual"]) and len(x) == 4:
        source = x[1]
        target = x[2]
        limit = x[3]
        return mkTag(("PayFeeResidual", [mkLimit(limit), source, target]))
    elif (x[0] in ["支付费用收益"] or x[0] in ["payFeeResidual"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("PayFeeResidual",[None, source, target, None]))
    elif (x[0] in ["计提支付利息"] or x[0] in ["accrueAndPayInt"]) and len(x) == 4:
        source = x[1]
        target = x[2]
        m = x[3]
        limit = getValWithKs(m, ['limit', "限制"])
        support = getValWithKs(m, ['support', "支持"])
        return mkTag(("AccrueAndPayInt", [mkLimit(limit), source, target, mkSupport(support)]))
    elif (x[0] in ["计提支付利息"] or x[0] in ["accrueAndPayInt"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("AccrueAndPayInt",[None, source, target, None]))
    elif (x[0] in ["支付利息"] or x[0] in ["payInt"]) and len(x) == 4:
        source = x[1]
        target = x[2]
        m = x[3]
        limit = getValWithKs(m, ['limit', "限制"])
        support = getValWithKs(m, ['support', "支持"])
        return mkTag(("PayInt", [mkLimit(limit), source, target, mkSupport(support)]))
    elif (x[0] in ["支付利息"] or x[0] in ["payInt"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("PayInt",[None, source, target, None]))
    elif (x[0] in ["顺序支付本金"] or x[0] in ["payPrinBySeq"]) and len(x) == 4:
        source = x[1]
        target = x[2]
        m = x[3]
        limit = getValWithKs(m, ['limit', "限制"])
        support = getValWithKs(m, ['support', "支持"])
        return mkTag(("PayPrinBySeq", [mkLimit(limit), source, target, mkSupport(support)]))
    elif (x[0] in ["顺序支付本金"] or x[0] in ["payPrinBySeq"]) and len(x)==3:
        source = x[1]
        target = x[2]
        return mkTag(("PayPrinBySeq",[None, source, target, None]))
    elif (x[0] in ["支付本金"] or x[0] in ["payPrin"]) and len(x) == 4:
        source = x[1]
        target = x[2]
        m = x[3]
        limit = getValWithKs(m, ['limit', "限制"])
        support = getValWithKs(m, ['support', "支持"])
        return mkTag(("PayPrin", [mkLimit(limit), source, target, mkSupport(support)]))
    elif (x[0] in ["支付本金"] or x[0] in ["payPrin"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("PayPrin",[None, source, target, None]))
    elif (x[0] in ["支付剩余本金"] or x[0] in ["payPrinResidual"]) and len(x) ==3:
        source = x[1]
        target = x[2]
        return mkTag(("PayPrinResidual",[source, target]))
    elif (x[0] in ["支付收益"] or x[0] in ["payIntResidual"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        limit = getValWithKs(m,['limit',"限制"])
        return mkTag(("PayIntResidual",[mkLimit(limit), source, target]))
    elif (x[0] in ["支付收益"] or x[0] in ["payIntResidual"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("PayIntResidual",[source, target]))
    elif (x[1] in ["出售资产"] or x[0] in ["sellAsset"]) and len(x) == 3:
        liq = x[1]
        target = x[2]
        return mkTag(("LiquidatePool",[mkLiqMethod(liq), target]))
    elif (x[0] in ["流动性支持"] or x[0] in ["liqSupport"]) and len(x) == 5:
        source = x[1]
        liqType = x[2]
        target = x[3]
        limit = x[4]
        return mkTag(("LiqSupport",[mkLimit(limit), source, mkLiqDrawType(liqType), target]))
    elif (x[0] in ["流动性支持"] or x[0] in ["liqSupport"]) and len(x) == 4:
        source = x[1]
        liqType = x[2]
        target = x[3]
        return mkTag(("LiqSupport",[None, source, mkLiqDrawType(liqType), target]))
    elif (x[0] in ["流动性支持偿还"] or x[0] in ["liqRepay"]) and len(x) == 4:
        rpt = x[1]
        source = x[2]
        target = x[3]
        return mkTag(("LiqRepay",[None, mkLiqRepayType(rpt), source, target]))
    elif (x[0] in ["流动性支持偿还"] or x[0] in ["liqRepay"]) and len(x) == 5:
        rpt = x[1]
        source = x[2]
        target = x[3]
        limit = x[4]
        return mkTag(("LiqRepay",[mkLimit(limit), mkLiqRepayType(rpt), source, target]))
    elif (x[0] in ["流动性支持报酬"] or x[0] in ["liqRepayResidual"]) and len(x) == 3:
        source = x[1]
        target = x[2]
        return mkTag(("LiqYield",[None, source, target]))
    elif (x[0] in ["流动性支持报酬"] or x[0] in ["liqRepayResidual"]) and len(x) == 4:
        source = x[1]
        target = x[2]
        limit = x[3]
        return mkTag(("LiqYield",[mkLimit(limit), source, target]))
    elif (x[0] in ["流动性支持计提"] or x[0] in ["liqAccrue"]) and len(x) == 2:
        target = x[1]
        return mkTag(("LiqAccrue", target))
    elif (x[0] in ["结算"] or x[0] in ["settleSwap"]) and len(x) == 3:
        acc = x[1]
        swapName = x[2]
        return mkTag(("SwapSettle", [acc, swapName]))
    elif (x[0] in ["条件执行"] or x[0] in ["If"]) and len(x) == 3:
        pre = x[1]
        actions = [x[2]]
        return mkTag(("ActionWithPre", [mkPre(pre), [mkAction(a) for a in actions]]))
    elif (x[0] in ["条件执行2"] or x[0] in ["IfElse"]) and len(x) == 4:
        pre = x[1]
        actions = x[2]
        actions2 = x[3]
        return mkTag(("ActionWithPre2", [mkPre(pre), [mkAction(a) for a in actions1], [mkAction(a) for a in actions2]]))
    elif (x[0] in ["购买资产"] or x[0] in ["buyAsset"]) and len(x) == 4:
        liq = x[1]
        source = x[2]
        _limit = x[3]
        return mkTag(("BuyAsset", [mkLimit(_limit), mkLiqMethod(liq), source]))
    elif (x[0] in ["购买资产"] or x[0] in ["buyAsset"]) and len(x) == 3:
        liq = x[1]
        source = x[2]
        return mkTag(("BuyAsset", [None, mkLiqMethod(liq), source]))
    elif (x[0] in ["更新事件"] or x[0] in ["runTrigger"]) and len(x) == 2:
        trgName = z[1]
        dealCycleM = chinaDealCycle | englishDealCycle
        return mkTag(("RunTrigger", ["InWF", trgName]))
    elif (x[0] in ["查看"] or x[0] in ["inspect"]) and len(x) == 3:
        comment = x[1]
        ds = [x[2]]
        return mkTag(("WatchVal", [comment, [mkDs(_) for _ in ds]]))

    else:
        raise RuntimeError(f"Failed to match :{x}:mkAction")

def mkStatus(x:Union[tuple,str]):
    if isinstance(x, str):
        if x in ["摊销", "Amortizing"]:
            return mkTag(("Amortizing",))
        elif x in ["循环", "Revolving"]:
            return mkTag(("Revolving",))
        elif x == "RampUp":
            return mkTag(("RampUp",))
        elif x in ["加速清偿", "Accelerated"]:
            return mkTag(("DealAccelerated", None))
        elif x in ["违约", "Defaulted"]:
            return mkTag(("DealDefaulted", None))
        elif x in ["结束", "Ended"]:
            return mkTag(("Ended",))
    
    elif isinstance(x, tuple):
        if len(x) == 2:
            status_type, status_value = x
            if status_type in ["设计", "PreClosing", "preclosing"]:
                return mkTag(("PreClosing", mkStatus(status_value)))

    else:
        raise RuntimeError(f"Failed to match :{x}:mkStatus")


def readStatus(x, locale):
    m = {"en": {'amort': "Amortizing", 'def': "Defaulted", 'acc': "Accelerated", 'end': "Ended",
                'called': "Called",
                'pre': "PreClosing",'revol':"Revolving"
                ,'ramp':"RampUp"}
        , "cn": {'amort': "摊销", 'def': "违约", 'acc': "加速清偿", 'end': "结束", 'pre': "设计","revol":"循环"
                 ,'called':"清仓回购"
                 ,'ramp':"RampUp"}}
    if x["tag"] == "Amortizing":
        return m[locale]['amort']
    elif x["tag"]=="DealAccelerated":
        return m[locale]['acc']
    elif x["tag"]=="DealDefaulted":
        return m[locale]['def']
    elif x["tag"]=="Ended":
        return m[locale]['end']
    elif x["tag"]== "PreClosing":
        return m[locale]['pre']
    elif x["tag"] == "Revolving":
        return m[locale]['revol']
    elif x["tag"] == "Called":
        return m[locale]['called']
    elif x["tag"]=="RampUp":
        return m[locale]['ramp']
    else:
        raise RuntimeError(
                f"Failed to read deal status:{x} with locale: {locale}")

def mkThreshold(x):
    if x == ">":
        return "Above"
    elif x == ">=":
        return "EqAbove"
    elif x == "<":
        return "Below"
    elif x == "<=":
        return "EqBelow"
    else:
        raise RuntimeError(f"Failed to match :{x}:mkThreshold")


def _rateTypeDs(x):
    h = x[0]
    if h in set(["资产池累积违约率"
                 , "cumPoolDefaultedRate"
                 , "债券系数"
                 , "bondFactor"
                 , "资产池系数"
                 , "poolFactor"]):
        return True
    return False


def mkTrigger(x:dict):
    if x == {"condition":p,"effects":e,"status":st,"curable":c} | {"条件":p,"效果":e,"状态":st,"重置":c}:
        triggerName = getValWithKs(x,["name","名称"],defaultReturn="")
        return {"trgName":triggerName
                    ,"trgCondition":mkPre(p)
                    ,"trgEffects":mkTriggerEffect(e)
                    ,"trgStatus":st
                    ,"trgCurable":c}
    else:
        raise RuntimeError(f"Failed to match :{x}:mkTrigger")


def mkTriggerEffect(x):
    if x == ("新状态", s) | ("newStatus", s):
        return mkTag(("DealStatusTo", mkStatus(s)))
    elif x == ["计提费用", *fn] | ["accrueFees", *fn]:
        return mkTag(("DoAccrueFee", fn))
    elif x == ["新增事件", trg] | ["newTrigger", trg]: # not implementd in Hastructure
        return mkTag(("AddTrigger", mkTrigger(trg)))
    elif x == ["新储备目标",accName,newReserve] | ["newReserveBalance",accName,newReserve]:
        return mkTag(("ChangeReserveBalance",[accName, mkAccType(newReserve)]))
    elif x == ["结果", *efs] | ["Effects", *efs]:
        return mkTag(("TriggerEffects", [mkTriggerEffect(e) for e in efs]))
    elif x == None:
        return mkTag(("DoNothing"))
    else:
        raise RuntimeError(f"Failed to match :{x}:mkTriggerEffect")


def mkWaterfall(r, x):
    mapping = {
        "未违约": "Amortizing",
        "摊销": "Amortizing",
        "循环": "Revolving",
        "加速清偿": "DealAccelerated",
        "违约": "DealDefaulted",
        "未设立": "PreClosing",
    }

    if len(x) == 0:
        return {k: list(v) for k, v in r.items()}

    _k, _v = x.popitem()
    _w_tag = None

    if _k in [("兑付日", "加速清偿"), ("amortizing", "accelerated"), "Accelerated"]:
        _w_tag = f"DistributionDay (DealAccelerated Nothing)"
    elif _k in [("兑付日", "违约"), ("amortizing", "defaulted"), "Defaulted"]:
        _w_tag = f"DistributionDay (DealDefaulted Nothing)"
    elif isinstance(_k, tuple) and len(_k) == 2 and _k[0] in ("兑付日", "amortizing"):
        _w_tag = f"DistributionDay {mapping.get(_k[1], _k[1])}"
    elif _k in ("兑付日", "未违约", "amortizing", "Amortizing"):
        _w_tag = f"DistributionDay Amortizing"
    elif _k in ("清仓回购", "cleanUp"):
        _w_tag = "CleanUp"
    elif _k in ("回款日", "回款后", "endOfCollection"):
        _w_tag = "EndOfPoolCollection"
    elif _k in ("设立日", "closingDay"):
        _w_tag = "OnClosingDay"
    elif _k in ("默认", "default"):
        _w_tag = "DefaultDistribution"
    else:
        raise RuntimeError(f"Failed to match :{_k}:mkWaterfall")
    r[_w_tag] = [mkAction(_a) for _a in _v]
    return mkWaterfall(r, x)

def mkRoundingType(x):
    if x == ["floor", r]:
        return mkTag(("RoundFloor", r))
    elif x == ["ceiling", r]:
        return mkTag(("RoundCeil", r))
    else:
        raise RuntimeError(f"Failed to match {x}:mkRoundingType")

def mkAssetRate(x):
    if x == ["固定", r] | ["fix", r]:
        return mkTag(("Fix", r))
    elif x == ["浮动", r, {"基准": idx, "利差": spd, "重置频率": p}]:
        _m = subMap(m, [("cap", None),( "floor", None),("rounding", None)])
        _m = applyFnToKey(_m, mkRoundingType, 'rounding')
        return mkTag(("Floater", [idx, spd, r, mkDatePattern(p), _m['floor'], _m['cap'],_m['rounding']]))
    elif x == ["floater", r, {"index": idx, "spread": spd, "reset": p}]:
        _m = subMap(m, [("cap", None), ("floor", None), ("rounding", None)])
        _m = applyFnToKey(_m, mkRoundingType, 'rounding')
        return mkTag(("Floater", [idx, spd, r, mkDatePattern(p), _m['floor'], _m['cap'],_m['rounding']]))
    else:
        raise RuntimeError(f"Failed to match {x}:mkAssetRate")

def mkAmortPlan(x) -> dict:
    if x == "等额本息" | "Level" | "level":
        return mkTag("Level")
    elif x == "等额本金" | "Even" | "even":
        return mkTag("Even")
    elif x == "先息后本" | "I_P" | "i_p":
        return mkTag("I_P")
    elif x == "等本等费" | "F_P" | "f_p":
        return mkTag("F_P")
    elif x == ("计划还款", ts, Dp) | ("Schedule", ts, Dp):
        return mkTag(("ScheduleRepayment", [mkTs("RatioCurve", ts), mkDatePattern(Dp)]))
    elif x == ("计划还款", ts) | ("Schedule", ts):
        return mkTag(("ScheduleRepayment", [mkTs("RatioCurve", ts), None]))
    else:
        raise RuntimeError(f"Failed to match AmortPlan {x}:mkAmortPlan")

def mkArm(x):
    if x == {"initPeriod": ip}:
        fc = x.get("firstCap", None)
        pc = x.get("periodicCap", None)
        floor = x.get("lifeFloor", None)
        cap = x.get("lifeCap", None)
        return mkTag(("ARM", [ip, fc, pc, cap, floor]))
    else:
        raise RuntimeError(f"Failed to match ARM  {x}:mkArm")

def mkAssetStatus(x):
    if x == "正常" | "Current" | "current":
        return mkTag(("Current"))
    elif x == "违约" | "Defaulted" | "defaulted":
        return mkTag(("Defaulted",None))
    elif x == ("违约", d) | ("Defaulted", d) | ("defaulted", d):
        return mkTag(("Defaulted", d))
    else:
        raise RuntimeError(f"Failed to match asset statuts {x}:mkAssetStatus")

def mkPrepayPenalty(x):
    if x is None:
        return None
    if x == {"byTerm": [term, rate1, rate2]} | {"按期限": [term, rate1, rate2]}:
        return mkTag(("ByTerm", [term, rate1, rate2]))
    elif x == {"fixAmount": [bal, term]} | {"固定金额": [bal, term]}:
        return mkTag(("FixAmount", [bal, term]))
    elif x == {"fixAmount": [bal]} | {"固定金额": [bal]}:
        return mkTag(("FixAmount", [bal, None]))
    elif x == {"fixPct": [pct, term]} | {"固定比例": [pct, term]}:
        return mkTag(("FixPct", [pct, term]))
    elif x == {"fixPct": [pct]} | {"固定比例": [pct]}:
        return mkTag(("FixPct", [pct, None]))
    elif x == {"sliding": [pct, step]} | {"滑动":[ pct, step]}:
        return mkTag(("Sliding", [pct, step]))
    elif x == {"stepDown": ps} | {"阶梯": [ps]}:
        return mkTag(("StepDown", ps))
    else:
        raise RuntimeError(f"Failed to match {x}:mkPrepayPenalty")

def mkAccRule(x):
    if x == "直线" | "Straight" :
        return "StraightLine"
    elif x == "余额递减" | "DecliningBalance" :
        return "DecliningBalance"
    else:
        raise RuntimeError(f"Failed to match {x}:mkAccRule")

def mkCapacity(x):
    if x[0] in ["固定", "Fixed"]:
        return mkTag(("FixedCapacity", x[1]))
    elif x[0] in ["按年限", "ByTerm"]:
        return mkTag(("CapacityByTerm", x[1]))
    else:
        raise RuntimeError(f"Failed to match {x}:mkCapacity")


def mkAsset(x):
    if x == ["AdjustRateMortgage", {"originBalance": originBalance, "originRate": originRate, "originTerm": originTerm, "freq": freq, "type": _type, "originDate": startDate, "arm": arm}
             , {"currentBalance": currentBalance, "currentRate": currentRate, "remainTerm": remainTerms, "status": status}]:
        borrowerNum = x[2].get("borrowerNum", None)
        prepayPenalty = getValWithKs(x[1],["prepayPenalty","早偿罚息"])
        return mkTag(("AdjustRateMortgage", [{"originBalance": originBalance,
                                                "originRate": mkRateType(originRate),
                                                "originTerm": originTerm,
                                                "period": freqMap[freq],
                                                "startDate": startDate,
                                                "prinType": mkAmortPlan(_type),
                                                "prepaymentPenalty": mkPrepayPenalty(prepayPenalty)
                                                } | mkTag("MortgageOriginalInfo"),
                                                mkArm(arm),
                                                currentBalance,
                                                currentRate,
                                                remainTerms,
                                                borrowerNum,
                                                mkAssetStatus(status)])) 
    elif x == ["按揭贷款", {"放款金额": originBalance, "放款利率": originRate, "初始期限": originTerm, "频率": freq, "类型": _type, "放款日": startDate}, {"当前余额": currentBalance, "当前利率": currentRate, "剩余期限": remainTerms, "状态": status}] | \
                ["Mortgage", {"originBalance": originBalance, "originRate": originRate, "originTerm": originTerm, "freq": freq, "type": _type, "originDate": startDate}, {"currentBalance": currentBalance, "currentRate": currentRate, "remainTerm": remainTerms, "status": status}]:

        borrowerNum = getValWithKs(x[2],["borrowerNum","借款人数量"])
        prepayPenalty = getValWithKs(x[1],["prepayPenalty","早偿罚息"])
        return mkTag(("Mortgage", [ {"originBalance": originBalance,
                                        "originRate": mkRateType(originRate),
                                        "originTerm": originTerm,
                                        "period": freqMap[freq],
                                        "startDate": startDate,
                                        "prinType": mkAmortPlan(_type),
                                        "prepaymentPenalty": mkPrepayPenalty(prepayPenalty)
                                        } | mkTag("MortgageOriginalInfo"),
                                        currentBalance,
                                        currentRate,
                                        remainTerms,
                                        borrowerNum,
                                        mkAssetStatus(status)]))
    elif x == ["贷款", {"放款金额": originBalance, "放款利率": originRate, "初始期限": originTerm, "频率": freq, "类型": _type, "放款日": startDate}, {"当前余额": currentBalance, "当前利率": currentRate, "剩余期限": remainTerms, "状态": status}] \
                | ["Loan", {"originBalance": originBalance, "originRate": originRate, "originTerm": originTerm, "freq": freq, "type": _type, "originDate": startDate}, {"currentBalance": currentBalance, "currentRate": currentRate, "remainTerm": remainTerms, "status": status}]:
        return mkTag(("PersonalLoan", [
                {"originBalance": originBalance,
                 "originRate": mkRateType(originRate),
                 "originTerm": originTerm,
                 "period": freqMap[freq],
                 "startDate": startDate,
                 "prinType": mkAmortPlan(_type)
                 } | mkTag("LoanOriginalInfo"),
                currentBalance,
                currentRate,
                remainTerms,
                mkAssetStatus(status)]))
    elif x == ["分期", {"放款金额": originBalance, "放款费率": originRate, "初始期限": originTerm, "频率": freq, "类型": _type, "放款日": startDate}, {"当前余额": currentBalance, "剩余期限": remainTerms, "状态": status}] \
                | ["Installment", {"originBalance": originBalance, "feeRate": originRate, "originTerm": originTerm, "freq": freq, "type": _type, "originDate": startDate}, {"currentBalance": currentBalance, "remainTerm": remainTerms, "status": status}]:
        return mkTag(("Installment", [
                {"originBalance": originBalance,
                 "originRate": mkRateType(originRate),
                 "originTerm": originTerm,
                 "period": freqMap[freq],
                 "startDate": startDate,
                 "prinType": mkAmortPlan(_type)
                 } | mkTag("LoanOriginalInfo"),
                currentBalance,
                remainTerms,
                mkAssetStatus(status)]))
    elif x == ["租赁", {"固定租金": dailyRate, "初始期限": originTerm, "频率": dp, "起始日": startDate, "状态": status, "剩余期限": remainTerms}] \
                | ["Lease", {"fixRental": dailyRate, "originTerm": originTerm, "freq": dp, "originDate": startDate, "status": status, "remainTerm": remainTerms}]:
        return mkTag(("RegularLease", [{"originTerm": originTerm, "startDate": startDate, "paymentDates": mkDatePattern(dp), "originRental": dailyRate} | mkTag("LeaseInfo"), 0, remainTerms, _statusMapping[status]]))
    elif x == ["租赁", {"初始租金": dailyRate, "初始期限": originTerm, "频率": dp, "起始日": startDate, "计提周期": accDp, "涨幅": rate, "状态": status, "剩余期限": remainTerms}] \
                | ["Lease", {"initRental": dailyRate, "originTerm": originTerm, "freq": dp, "originDate": startDate, "accrue": accDp, "pct": rate, "status": status, "remainTerm": remainTerms}]:

        dailyRatePlan = None
        _stepUpType = "curve" if isinstance(rate, list) else "constant"
        if _stepUpType == "constant":
            dailyRatePlan = mkTag(
                    ("FlatRate", [mkDatePattern(accDp), rate]))
        else:
            dailyRatePlan = mkTag(
                    ("ByRateCurve", [mkDatePattern(accDp), rate]))
        return mkTag(("StepUpLease", [{"originTerm": originTerm, "startDate": startDate, "paymentDates": mkDatePattern(dp), "originRental": dailyRate} | mkTag("LeaseInfo"), dailyRatePlan, 0, remainTerms, mkAssetStatus(status)]))
    elif x == ["固定资产",{"起始日":sd,"初始余额":ob,"初始期限":ot,"残值":rb,"周期":p,"摊销":ar,"产能":cap}
                      ,{"剩余期限":rt}] \
             |["FixedAsset",{"start":sd,"originBalance":ob,"originTerm":ot,"residual":rb,"period":p,"amortize":ar
                             ,"capacity":cap}
                           ,{"remainTerm":rt}]:
        return mkTag(("FixedAsset",[{"startDate":sd,"originBalance":ob,"originTerm":ot,"residualBalance":rb
                                         ,"period":freqMap[p],"accRule":mkAccRule(ar)
                                         ,"capacity":mkCapacity(cap)} | mkTag("FixedAssetInfo")
                                        ,rt]))
    else:
        raise RuntimeError(f"Failed to match {x}:mkAsset")


def identify_deal_type(x):
    if x.get("pool", {}).get("assets", []) and x["pool"]["assets"][0].get("tag") == 'PersonalLoan':
        return "LDeal"
    elif x.get("pool", {}).get("assets", []) and x["pool"]["assets"][0].get("tag") == 'Mortgage':
        return "MDeal"
    elif x.get("pool", {}).get("assets", []) and x["pool"]["assets"][0].get("tag") == 'AdjustRateMortgage':
        return "MDeal"
    elif x.get("pool", {}).get("assets", []) == [] and x.get("pool", {}).get("futureCf", [])[0].get("tag") == 'MortgageFlow':
        return "MDeal"
    elif x.get("pool", {}).get("assets", []) and x["pool"]["assets"][0].get("tag") == 'Installment':
        return "IDeal"
    elif x.get("pool", {}).get("assets", []) and (x["pool"]["assets"][0].get("tag") == 'Lease' or x["pool"]["assets"][0].get("tag") == 'RegularLease'):
        return "RDeal"
    elif x.get("pool", {}).get("assets", []) and x["pool"]["assets"][0].get("tag") == 'StepUpLease':
        return "RDeal"
    elif x.get("pool", {}).get("assets", []) and x["pool"]["assets"][0].get("tag") == 'FixedAsset':
        return "FDeal"
    else:
        raise RuntimeError(f"Failed to identify deal type {x}")

def mkCallOptions(x):
    if x == {"资产池余额": bal} | {"poolBalance": bal} |("poolBalance", bal) :
        return mkTag(("PoolBalance", bal))
    elif x == {"债券余额": bal} | {"bondBalance": bal} | ("bondBalance", bal):
        return mkTag(("BondBalance", bal))
    elif x == {"资产池余额剩余比率": factor} | {"poolFactor": factor} | ("poolFactor", factor):
        return mkTag(("PoolFactor", factor))
    elif x == {"债券余额剩余比率": factor} | {"bondFactor": factor} | ("bondFactor", factor):
        return mkTag(("BondFactor", factor))
    elif x == {"指定日之后": d} | {"afterDate": d} | ("afterDate", d):
        return mkTag(("AfterDate", d))
    elif x == ("判断", p) | ("条件",p) | ("if",p) | ("condition",p):
        return mkTag(("Pre", mkPre(p)))
    elif x == {"任意满足": xs} | {"or": xs} | ("any", *xs) | ("or", *xs):
        return mkTag(("Or", [mkCallOptions(_x) for _x in xs]))
    elif x == {"全部满足": xs} | {"and": xs} | ("all", *xs) | ("all", *xs):
        return mkTag(("And", [mkCallOptions(_x) for _x in xs]))
    else:
        raise RuntimeError(f"Failed to match {x}:mkCallOptions")

def mkAssumpDefault(x):
    ''' New default assumption for performing assets '''

    if isinstance(x, dict) and "CDR" in x and isinstance(x["CDR"], list):
        return mkTag(("DefaultVec", x["CDR"]))
    elif isinstance(x, dict) and "CDR" in x:
        return mkTag(("DefaultCDR", x["CDR"]))
    elif isinstance(x, dict) and "ByAmount" in x:
        bal, rs = x["ByAmount"]
        return mkTag(("DefaultByAmt", (bal, rs)))
    else:
        raise RuntimeError(f"failed to match {x}")


def mkAssumpPrepay(x):
    ''' New prepayment assumption for performing assets '''
    if x == {"CPR":r} :
        if isinstance(r,list):
            return mkTag(("PrepaymentVec",r))
    elif x == {"CPR":r} :
        return mkTag(("PrepaymentCPR",r))
    else:
        raise RuntimeError(f"failed to match {x}")

def mkAssumpDelinq(x):
    ''' New delinquency assumption for performing assets '''
    if x == {"DelinqCDR":cdr,"Lag":lag,"DefaultPct":pct}:
        return mkTag(("DelinqCDR",[cdr,(lag,pct)]))
    else:
        raise RuntimeError(f"failed to match {x}")

def mkAssumpLeaseGap(x):
    if x == {"Days":d}:
        return mkTag(("GapDays",d))
    elif x == {"DaysByAmount":(tbl,d)}:
        return mkTag(("GapDaysByAmount",[tbl,d]))
    else:
        raise RuntimeError(f"failed to match {x}")

def mkAssumpLeaseRent(x):
    if x == {"AnnualIncrease":r}:
        return mkTag(("BaseAnnualRate",r))
    elif x == {"CurveIncrease":r}:
        return mkTag(("BaseCurve",r))
    else:
        raise RuntimeError(f"failed to match {x}")

def mkAssumpRecovery(x):
    ''' recovery assumption for defaults from performing assets '''

    if isinstance(x, dict) and "Rate" in x and "Lag" in x:
        r, lag = x["Rate"], x["Lag"]
        return mkTag(("Recovery", [r, lag]))
    elif isinstance(x, dict) and "Rate" in x and "Timing" in x:
        r, ts = x["Rate"], x["Timing"]
        return mkTag(("RecoveryTiming", [r, ts]))
    else:
        raise RuntimeError(f"failed to match {x}")


def mkDefaultedAssumption(x):
    ''' default assumption for defaulted assets'''

    if isinstance(x, tuple) and len(x) == 4 and x[0] == "Defaulted":
        r, lag, rs = x[1:]
        return mkTag(("DefaultedRecovery", [r, lag, rs]))
    else:
        return mkTag(("DummyDefaultAssump"))


def mkDelinqAssumption(x):
    #return "DummyDelinqAssump"
    #return mkTag("DummyDelinqAssump")
    return []


def mkPerfAssumption(x):
    "Make assumption on performing assets"

    def mkExtraStress(y):
        ''' make extra stress for mortgage/loans '''
        if y is None:
            return None

        # ppy/default time-based stress
        defaultFactor = getValWithKs(y, ['defaultFactor', "违约因子"], mapping=mkFloatTs)
        prepayFactor = getValWithKs(y, ['prepayFactor', "早偿因子"], mapping=mkFloatTs)

        # haircuts
        mkHaircut = lambda xs: [(mkPoolSource(ps), r) for (ps, r) in xs]
        haircuts = getValWithKs(y, ['haircuts', 'haircut', "折扣"], mapping=mkHaircut)

        return {
            "defaultFactors": defaultFactor,
            "prepaymentFactors": prepayFactor,
            "poolHairCut": haircuts
        }

    if isinstance(x, tuple) and len(x) in [5, 6] and x[0] == "Mortgage":
        if x[1] == "Delinq":
            md, mp, mr, mes = x[2:]
            d = earlyReturnNone(mkAssumpDelinq, md)
            p = earlyReturnNone(mkAssumpPrepay, mp)
            r = earlyReturnNone(mkAssumpRecovery, mr)
            return mkTag(("MortgageDeqAssump", [d, p, r, mkExtraStress(mes)]))
        else:
            md, mp, mr, mes = x[1:]
            d = earlyReturnNone(mkAssumpDefault, md)
            p = earlyReturnNone(mkAssumpPrepay, mp)
            r = earlyReturnNone(mkAssumpRecovery, mr)
            return mkTag(("MortgageAssump", [d, p, r, mkExtraStress(mes)]))

    elif isinstance(x, tuple) and len(x) == 4 and x[0] == "Lease":
        gap, rent, endDate = x[1:]
        return mkTag(("LeaseAssump", [mkAssumpLeaseGap(gap), mkAssumpLeaseRent(rent), endDate, None]))

    elif isinstance(x, tuple) and len(x) == 5 and x[0] == "Loan":
        md, mp, mr, mes = x[1:]
        d = earlyReturnNone(mkAssumpDefault, md)
        p = earlyReturnNone(mkAssumpPrepay, mp)
        r = earlyReturnNone(mkAssumpRecovery, mr)
        return mkTag(("LoanAssump", [d, p, r, mkExtraStress(mes)]))

    elif isinstance(x, tuple) and len(x) == 5 and x[0] == "Installment":
        md, mp, mr, mes = x[1:]
        d = earlyReturnNone(mkAssumpDefault, md)
        p = earlyReturnNone(mkAssumpPrepay, mp)
        r = earlyReturnNone(mkAssumpRecovery, mr)
        return mkTag(("InstallmentAssump", [d, p, r, None]))

    elif isinstance(x, tuple) and len(x) == 3 and x[0] == "Fixed":
        utilCurve, priceCurve = x[1:]
        return mkTag(("FixedAssetAssump", [mkTs("RatioCurve", utilCurve), mkTs("BalanceCurve", priceCurve)]))

    else:
        raise RuntimeError(f"failed to match {x}")


def mkPDF(a,b,c):
    ''' make assumps asset with 3 status: performing/delinq/defaulted '''
    return [mkPerfAssumption(a),mkDelinqAssumption(b),mkDefaultedAssumption(c)]

def mkAssumpType(x):
    ''' make assumps either on pool level or asset level '''
    if x[0] == "Pool":
        return mkTag(("PoolLevel",mkPDF(x[1], x[2], x[3])))
    elif x[0] == "ByIndex":
        return mkTag(("ByIndex",[ [idx, mkPDF(a,b,c)] for (idx,(a,b,c)) in x[1]]))
    else :
        raise RuntimeError(f"failed to match {x} | mkAssumpType")

def mkAssetUnion(x):
    if x[0] == "AdjustRateMortgage" | "Mortgage" | "按揭贷款" :
        return mkTag(("MO",mkAsset(x)))
    elif x[0] == "贷款" | "Loan" : 
        return mkTag(("LO",mkAsset(x)))
    elif x[0] == "分期" | "Installment" : 
        return mkTag(("IL",mkAsset(x)))
    elif x[0] == "租赁" | "Lease" : 
        return mkTag(("LS",mkAsset(x)))
    elif x[0] == "固定资产" | "FixedAsset" : 
        return mkTag(("FA",mkAsset(x)))
    else:
        raise RuntimeError(f"Failed to match AssetUnion {x}")

def mkRevolvingPool(x):
    assert isinstance(x, list), f"Revolving Pool Assumption should be a list, but got {x}"
    'intpu with list, return revolving pool'
    if x == ["constant",*asts]|["固定",*asts]:
        return mkTag(("ConstantAsset",[ mkAssetUnion(_) for _ in asts]))
    elif x == ["static",*asts]|["静态",*asts]:
        return mkTag(("StaticAsset",[ mkAssetUnion(_) for _ in asts]))
    elif x == ["curve",astsWithDates]|["曲线",astsWithDates]:
        assetCurve = [ [d, [mkAssetUnion(a) for a in asts]] for (d,asts) in astsWithDates ]            
        return mkTag(("AssetCurve",assetCurve))

def mkAssumpList(xs):
    assert isinstance(xs, list), f"Assumption should be a list, but got {xs}"
    return [ mkAssumption(x) for x in xs ]

def mkAssumption2(x) -> dict:
    if x == ["ByIndex", assetAssumpList, dealAssump] | ["明细", assetAssumpList, dealAssump]:
        return mkTag(("ByIndex"
                         , [[(ids, mkAssumpList(aps)) for ids, aps in assetAssumpList]
                         , mkAssumpList(dealAssump)]))
    elif x == xs: 
        if isinstance(xs, list):
            return mkTag(("PoolLevel", mkAssumpList(xs)))
    elif x == None:
        return mkTag(("PoolLevel", []))
    else:
        raise RuntimeError(f"Failed to match {x}:mkAssumption2, type:{type(x)}")


def mkPool(x):
    mapping = {"LDeal": "LPool", "MDeal": "MPool",
               "IDeal": "IPool", "RDeal": "RPool"}
    if x == {"清单": assets, "封包日": d} | {"assets": assets, "cutoffDate": d}:
        _pool = {"assets": [mkAsset(a) for a in assets]
                     , "asOfDate": d}
            
        _pool_asset_type = identify_deal_type({"pool": _pool})
        return mkTag((mapping[_pool_asset_type], _pool))
    else:
        raise RuntimeError(f"Failed to match {x}:mkPool")


def mkCustom(x):
    if x == {"常量": n} | {"Constant": n}:
        return mkTag(("CustomConstant", n))
    elif x == {"余额曲线": ts} | {"BalanceCurve": ts}:
        return mkTag(("CustomCurve", mkTs("BalanceCurve", ts)))
    elif x == {"公式": ds} | {"Formula": ds}:
        return mkTag(("CustomDS", mkDs(ds)))


def mkLiqProviderType(x):
    if x == {"总额度": amt} | {"total": amt}:
        return mkTag(("FixSupport", amt))
    elif x == {"日期": dp, "限额": amt} | {"reset": dp, "quota": amt}:
        return mkTag(("ReplenishSupport", [mkDatePattern(dp), amt]))
    elif x == {"公式": ds, "系数":pct} | {"formula":ds, "pct":pct}:
        return mkTag(("ByPct", [mkDs(ds), pct]))
    elif x == {}:
        return mkTag(("UnLimit"))
    else:
        raise RuntimeError(f"Failed to match LiqProvider Type：{x}")
        
def mkLiqProvider(n, x):
    opt_fields = {"liqCredit":None,"liqDueInt":0,"liqDuePremium":0
                 ,"liqRate":None,"liqPremiumRate":None,"liqStmt":None
                 ,"liqBalance":0,"liqRateType":None,"liqPremiumRateType":None
                 ,"liqDueIntDate":None,"liqEnds":None}

    x_transformed = renameKs(x,[("已提供","liqBalance"),("应付利息","liqDueInt"),("应付费用","liqDuePremium")
                                ,("利率","liqRate"),("费率","liqPremiumRate"),("记录","liqStmt"),
                                ]
                                ,opt_key=True)
    r = None
    if x_transformed == {"类型": "无限制", "起始日": _sd, **p} | {"type": "Unlimited", "start": _sd, **p}:
        r = {"liqName": n, "liqType": mkLiqProviderType({})
                ,"liqBalance": p.get("balance",0), "liqStart": _sd
                ,"liqRateType": mkRateType(p.get("rate",None))
                ,"liqPremiumRateType": mkRateType(p.get("fee",None))
                } 
    elif x_transformed == {"类型": _sp, "额度": _ab, "起始日": _sd, **p} \
                | {"type": _sp, "lineOfCredit": _ab, "start": _sd, **p}:
        r = {"liqName": n, "liqType": mkLiqProviderType(_sp)
                ,"liqBalance": _ab,  "liqStart": _sd
                ,"liqRateType": mkRateType(p.get("rate",None))
                ,"liqPremiumRateType": mkRateType(p.get("fee",None))
                } 
    elif x_transformed == {"额度": _ab, "起始日": _sd, **p} | {"lineOfCredit": _ab, "start": _sd, **p}:
        r = {"liqName": n, "liqType": mkTag(("FixSupport",_ab))
                ,"liqBalance": _ab,  "liqStart": _sd
                ,"liqRateType": mkRateType(p.get("rate",None))
                ,"liqPremiumRateType": mkRateType(p.get("fee",None))
                } 
        
    else:
        raise RuntimeError(f"Failed to match LiqProvider:{x}")

    if r is not None:
       return opt_fields | r 

def mkLedger(n, x):
    if x == {"balance":bal} | {"余额":bal}:
        return {"ledgName":n,"ledgBalance":bal,"ledgStmt":None}
    elif x == {"balance":bal,"txn":_tx} | {"余额":bal,"记录":_tx}:
        tx = mkAccTxn(_tx)
        return {"ledgName":n,"ledgBalance":bal,"ledgStmt":tx}
    else:
        raise RuntimeError(f"Failed to match Ledger:{n},{x}")

def mkCf(x):
    if len(x) == 0:
        return None
    else:
        return [mkTag(("MortgageFlow", _x+[0.0]*5+[None,None,None])) for _x in x]


def mkCollection(x):
    """ Build collection rules """
    if isinstance(x, list):
        if len(x) == 2 and isinstance(x[1], str):
            return mkTag(("Collect", [mkPoolSource(x[0]), x[1]]))
        elif len(x) > 1 and isinstance(x[1:], list):
            return mkTag(("CollectByPct", [mkPoolSource(x[0]), x[1:]]))
    raise RuntimeError(f"Failed to match collection rule {x}")


def mk(x):
    if x == ["资产", assets]:
        return {"assets": [mkAsset(a) for a in assets]}
    elif x == ["账户", accName, attrs] | ["account", accName, attrs]:
        return {accName: mkAcc(accName, attrs)}

def mkFee(x, fsDate=None):
    if "name" in x and "type" in x:
        fn = x["name"]
        feeType = x["type"]
        fi = {key: value for key, value in x.items() if key not in ["name", "type"]}
        opt_fields = subMap(fi, [("feeStart", fsDate), ("feeDueDate", None), ("feeDue", 0),
                                ("feeArrears", 0), ("feeLastPaidDate", None)])
        return {"feeName": fn, "feeType": mkFeeType(feeType)} | opt_fields
    elif "名称" in x and "类型" in x:
        fn = x["名称"]
        feeType = x["类型"]
        fi = {key: value for key, value in x.items() if key not in ["名称", "类型"]}
        opt_fields = subMap2(fi, [("起算日", "feeStart", fsDate), ("计算日", "feeDueDate", None), ("应计费用", "feeDue", 0),
                                  ("拖欠", "feeArrears", 0), ("上次缴付日期", "feeLastPaidDay", None)])
        return {"feeName": fn, "feeType": mkFeeType(feeType)} | opt_fields
    else:
        raise RuntimeError(f"Failed to match fee: {x}")


def mkPricingAssump(x):
    if x == {"贴现日": pricingDay, "贴现曲线": xs} | {"date": pricingDay, "curve": xs}| {"PVDate": pricingDay, "PVCurve": xs}:
        return mkTag(("DiscountCurve", [pricingDay, mkTs("IRateCurve", xs)]))
    elif x == {"债券": bnd_with_price, "利率曲线": rdps} | {"bonds": bnd_with_price, "curve": rdps}:
        return mkTag(("RunZSpread", [mkTs("IRateCurve", rdps), bnd_with_price]))
    else:
        raise RuntimeError(f"Failed to match pricing assumption: {x}")

def readPricingResult(x, locale) -> dict:
    if x is None:
        return None
    h = None

    tag = list(x.values())[0]["tag"]
    if tag == "PriceResult":
        h = {"cn": ["估值", "票面估值", "WAL", "久期", "凸性", "应计利息"],
             "en": ["pricing", "face", "WAL", "duration", "convexity", "accure interest"]}
    elif tag == "ZSpread":
        h = {"cn": ["静态利差"], "en": ["Z-spread"]}
    else:
        raise RuntimeError(
            f"Failed to read princing result: {x} with tag={tag}")

    return pd.DataFrame.from_dict({k: v['contents'] for k, v in x.items()}, orient='index', columns=h[locale]).sort_index()


def readRunSummary(x, locale) -> dict:
    def filter_by_tags(xs, tags):
        tags_set = set(tags)
        return [ x for x in xs if x['tag'] in tags_set]

    r = {}
    if x is None:
        return None

    bndStatus = {'cn': ["本金违约", "利息违约", "起算余额"]
                ,'en': ["Balance Defaults", "Interest Defaults", "Original Balance"]}
    bond_defaults = [(_['contents'][0], _['tag'], _['contents'][1], _['contents'][2])
                     for _ in x if _['tag'] in set(['BondOutstanding', 'BondOutstandingInt'])]
    _fmap = {"cn": {'BondOutstanding': "本金违约", "BondOutstandingInt": "利息违约"}
            ,"en": {'BondOutstanding': "Balance Defaults", "BondOutstandingInt": "Interest Defaults"}}
    ## Build bond summary
    bndNames = set([y[0] for y in bond_defaults])
    bndSummary = pd.DataFrame(columns=bndStatus[locale], index=list(bndNames))
    for bn, amt_type, amt, begBal in bond_defaults:
        bndSummary.loc[bn][_fmap[locale][amt_type]] = amt
        bndSummary.loc[bn][bndStatus[locale][2]] = begBal
    bndSummary.fillna(0, inplace=True)
    bndSummary["Total"] = bndSummary[bndStatus[locale][0]] + \
        bndSummary[bndStatus[locale][1]]

    r['bonds'] = bndSummary
    ## Build status change logs
    dealStatusLog = {'cn': ["日期", "旧状态", "新状态"], 'en': ["Date", "From", "To"]}
    status_change_logs = [(_['contents'][0], readStatus(_['contents'][1], locale), readStatus(_['contents'][2], locale))
                          for _ in x if _['tag'] in set(['DealStatusChangeTo'])]
    r['status'] = pd.DataFrame(data=status_change_logs, columns=dealStatusLog[locale])

    # inspection variables
    def uplift_ds(df):
        ds_name = readTagStr(df['DealStats'].iloc[0])
        df.drop(columns=["DealStats"],inplace=True)
        df.rename(columns={"Value":ds_name},inplace=True)
        df.set_index("Date",inplace=True)
        return df
    inspect_vars = filter_by_tags(x, ["InspectBal","InspectBool","InspectRate"])
    if inspect_vars:
        inspect_df = pd.DataFrame(data = [ (c['contents'][0],str(c['contents'][1]),c['contents'][2]) for c in inspect_vars ]
                                ,columns = ["Date","DealStats","Value"])
        grped_inspect_df = inspect_df.groupby("DealStats")

        r['inspect'] = {readTagStr(k):uplift_ds(v) for k,v in grped_inspect_df}

    # inspect variables during waterfall
    r['waterfallInspect'] = None
    waterfall_inspect_vars = filter_by_tags(x, ["InspectWaterfall"])
    if waterfall_inspect_vars:
        waterfall_inspect_df = pd.DataFrame(data = [ (c['contents'][0],str(c['contents'][1]),ds,dsv) 
                                                        for c in waterfall_inspect_vars
                                                         for (ds,dsv) in zip(c['contents'][2],c['contents'][3]) ]
                                            ,columns = ["Date","Comment","DealStats","Value"])
        r['waterfallInspect'] = waterfall_inspect_df
    
    # extract errors and warnings
    error_warning_logs = filter_by_tags(x, ["ErrorMsg","WarningMsg"])
    r['logs'] = None
    if error_warning_logs:
        errorLogs = [ ["Error",c['contents']] for c in error_warning_logs if c['tag']=="ErrorMsg"]
        warningLogs = [ ["Warning",c['contents']] for c in error_warning_logs if c['tag']=="WarningMsg"]
        r['logs'] = pd.DataFrame(data = errorLogs+warningLogs ,columns = ["Type","Comment"])

    # build financial reports
    def mapItem(z):
        if z == {"tag":"Item","contents":[accName,accBal]}:
            return {accName:accBal}
        elif z == {"tag":"ParentItem","contents":[accName,subItems]}:
            items = [ mapItem(i) for i in subItems]
            return {accName : items}

    def buildBalanceSheet(bsData):
        bsRptDate = bsData.pop("reportDate")
        bs = mapListValBy(bsData, mapItem)
        return mapValsBy(bs, uplift_m_list) | {"reportDate":bsRptDate}

    def buildBsType(yname, y:dict)-> pd.DataFrame:
        mi = pd.MultiIndex.from_product([[yname],y.keys()])
        d = y.values()
        return pd.DataFrame(d, index=mi).T
    
    def buildBS(bs):
        bs_df = pd.concat([  buildBsType(k,v)  for k,v in bs.items() if k!="reportDate"],axis=1)
        bs_df['reportDate'] = bs['reportDate']
        return bs_df.set_index("reportDate")

    def buildCashReport(cashData):
        sd = cashData.pop('startDate')
        ed = cashData.pop('endDate')
        net = cashData.pop('net')
        cashList = mapListValBy(cashData, mapItem)
        cashMap = {k:uplift_m_list(v) for k,v in cashList.items() }
        cashMap = pd.concat([  buildBsType(k,v) for k,v in cashMap.items() ],axis=1)
        cashMap['startDate'] = sd
        cashMap['endDate'] = ed
        cashMap['Net'] = net
        return cashMap.set_index(["startDate","endDate"])

    balanceSheetIdx = 2
    cashReportIdx = 3
    rpts = [ _['contents'] for  _ in  (filter_by_tags(x, ["FinancialReport"])) ]
    if rpts:
        r['report'] = {}
        r['report']['balanceSheet'] = pd.concat([buildBS(buildBalanceSheet(rpt[balanceSheetIdx])) for rpt in rpts])
        r['report']['cash'] = pd.concat([buildCashReport(rpt[cashReportIdx]) for rpt in rpts])[["inflow","outflow","Net"]]
    

    return r


def aggAccs(x, locale):
    header = accountHeader[locale]
    agg_acc = {}
    for k, v in x.items():
        acc_by_date = v.groupby(header["idx"])
        acc_txn_amt = acc_by_date.agg(change=(header["change"], sum)).rename(columns={"change":header["change"]})
        
        ending_bal_column = acc_by_date.last()[header["bal"][1]].rename(header["bal"][2])
        begin_bal_column = ending_bal_column.shift(1).rename(header["bal"][0])
        
        agg_acc[k] = acc_txn_amt.join([begin_bal_column, ending_bal_column])
        if agg_acc[k].empty:
            agg_acc[k].columns = header["bal"][0], header['change'], header["bal"][2]
            continue
        fst_idx = agg_acc[k].index[0]
        agg_acc[k].at[fst_idx, header["bal"][0]] = round(agg_acc[k].at[fst_idx,  header["bal"][2]] - agg_acc[k].at[fst_idx, header['change']], 2)
        agg_acc[k] = agg_acc[k][[header["bal"][0], header['change'], header["bal"][2]]]

    return agg_acc


def readCutoffFields(pool):
    _map = {'cn': "发行", 'en': "Issuance"}

    lang_flag = None
    if '发行' in pool.keys():
        lang_flag = 'cn'
    elif 'Issuance' in pool.keys():
        lang_flag = 'en'
    else:
        return None

    validCutoffFields = {
        "资产池规模": "IssuanceBalance"
        ,"IssuanceBalance": "IssuanceBalance"
        ,"CumulativeDefaults":"HistoryDefaults"
        ,"累计违约余额":"HistoryDefaults"

    }

    r = {}
    for k, v in pool[_map[lang_flag]].items():
        if k in validCutoffFields:
            r[validCutoffFields[k]] = v
        else:
            logging.warning(f"Key {k} is not in pool fields {validCutoffFields.keys()}")
    return r

def mkRateAssumption(x):
    if x == (idx,r):
        if isinstance(r, list):
            return mkTag(("RateCurve",[idx, mkCurve("IRateCurve",r)]))
    elif x == (idx,r) :
        return mkTag(("RateFlat" ,[idx, r]))
    else :
        raise RuntimeError(f"Failed to match RateAssumption:{x}")

def mkNonPerfAssumps(r, xs:list) -> dict:
    def translate(y):
        if y == ("stop",d):
            return {"stopRunBy":d}
        elif y == ("estimateExpense",*projectExps):
            return {"projectedExpense":[(fn,mkTs("BalanceCurve",ts)) for (fn,ts) in projectExps]}
        elif y == ("call",*opts):
            return {"callWhen":[mkCallOptions(opt) for opt in opts]}
        elif y == ("revolving",rPool,rPerf):
            return {"revolving":mkTag(("AvailableAssets", [mkRevolvingPool(rPool), mkAssumpType(rPerf)]))}
        elif y == ("interest",*ints):
            return {"interest":[mkRateAssumption(_) for _ in ints]}
        elif y == ("inspect",*tps):
            return {"inspectOn":[ (mkDatePattern(dp),mkDs(ds)) for (dp,ds) in tps]}
        elif y == ("report",m):
            interval = m['dates']
            return {"buildFinancialReport":mkDatePattern(interval)}
        elif y == ("pricing",p):
            return {"pricing":mkPricingAssump(p)}
        elif y == ("fireTrigger",scheduleFired):
            return {"fireTrigger":[ (dt,dealCycleMap[cyc],tn) for (dt,cyc,tn) in scheduleFired]}
    if xs == None:
        return {}
    elif xs == []:
        return r
    elif xs == [x,*rest]:
        return mkNonPerfAssumps(r | translate(x),rest)

def show(r, x="full"):
    ''' show cashflow of SPV during the projection '''
    def _map(y):
        if y == 'cn':
            return {"agg_accounts": "账户", "fees": "费用", "bonds": "债券", "pool": "资产池", "idx": "日期"}
        else:
            return {"agg_accounts": "Accounts", "fees": "Fees", "bonds": "Bonds", "pool": "Pool", "idx": "date"}

    _comps = ['agg_accounts', 'fees', 'bonds']

    dfs = {c: pd.concat(r[c].values(), axis=1, keys=r[c].keys())
           for c in _comps if r[c]}

    locale = guess_locale(r)
    _m = _map(locale)

    dfs2 = {}

    for k, v in dfs.items():
        dfs2[_m[k]] = pd.concat([v], keys=[_m[k]], axis=1)

    agg_pool = pd.concat([r['pool']['flow']], axis=1, keys=[_m["pool"]])
    agg_pool = pd.concat([agg_pool], axis=1, keys=[_m["pool"]])

    _full = functools.reduce(lambda acc, x: acc.merge(
        x, how='outer', on=[_m["idx"]]), [agg_pool]+list(dfs2.values()))

    if x == "full":
        return _full.loc[:, [_m["pool"]]+list(dfs2.keys())].sort_index()
    elif x == "cash":
        return None  # ""


