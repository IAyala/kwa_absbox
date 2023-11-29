import json
#from pyspecter import S,query
from absbox.local.util import flat


def inSet(xs,validSet):
    if set(xs).issubset(validSet):
        return True
    else:
        return False

def valDeal(d, error, warning) -> list:
    acc_names = set(d['accounts'].keys())
    bnd_names = set(d['bonds'].keys())
    fee_names = set(d['fees'].keys())
    swap_names = set([]) if d['rateSwap'] is None else set(d['rateSwap'].keys())
    w = d['waterfall']    
    
    # optional
    if d['ledgers']:
        ledger_names = set(d['ledgers'].keys())
    
    assert w is not None, "Waterfall is None"

    def validateAction(action):
        rnt = ""
        if action.get("tag") == 'PayFee' and len(action.get("contents", [])) == 4:
            _, acc, fees, _ = action["contents"]
            if not inSet([acc], acc_names):
                rnt += f"Account {acc} is not in deal account {acc_names};"
            if not inSet(fees, fee_names):
                rnt += f"Fees {fees} is not in deal fees {fee_names};"
        
        elif action.get("tag") == 'CalcAndPayFee' and len(action.get("contents", [])) == 4:
            _, acc, fees, _ = action["contents"]
            if not inSet([acc], acc_names):
                rnt += f"Account {acc} is not in deal account {acc_names};"
            if not inSet(fees, fee_names):
                rnt += f"Fees {fees} is not in deal fees {fee_names};"            
        
        elif action.get("tag") in ('PayInt', 'AccrueAndPayInt') and len(action.get("contents", [])) == 4:
            _, acc, bonds, _ = action["contents"]
            if not inSet([acc], acc_names):
                rnt += f"Account {acc} is not in deal account {acc_names};"
            if not inSet(bonds, bnd_names):
                rnt += f"Bonds {bonds} is not in deal bonds {bnd_names};"
        
        elif action.get("tag") == 'PayPrin' and len(action.get("contents", [])) == 4:
            _, acc, bonds, _ = action["contents"]
            if not inSet([acc], acc_names):
                rnt += f"Account {acc} is not in deal account {acc_names};"
            if not inSet(bonds, bnd_names):
                rnt += f"Bonds {bonds} is not in deal bonds {bnd_names};"
        
        elif action.get("tag") == 'PayPrinResidual' and len(action.get("contents", [])) == 2:
            acc, bonds = action["contents"]
            if not inSet([acc], acc_names):
                rnt += f"Account {acc} is not in deal account {acc_names};"
            if not inSet(bonds, bnd_names):
                rnt += f"Bonds {bonds} is not in deal bonds {bnd_names};"
        
        elif action.get("tag") == 'Transfer' and len(action.get("contents", [])) == 4:
            _, acc1, acc2, _ = action["contents"]
            if not inSet([acc1, acc2], acc_names):
                rnt += f"Account {acc1, acc2} is not in deal account {acc_names};"
        
        elif action.get("tag") == 'PayFeeResidual' and len(action.get("contents", [])) == 3:
            _, acc, fee = action["contents"]
            if not inSet([acc], acc_names):
                rnt += f"Account {acc} is not in deal account {acc_names};"
            if not inSet([fee], fee_names):
                rnt += f"Fee {fee} is not in deal fees {fee_names};"
        
        elif action.get("tag") == 'PayIntResidual' and len(action.get("contents", [])) == 3:
            _, acc, bnd_name = action["contents"]
            if not inSet([acc], acc_names):
                rnt += f"Account {acc} is not in deal account {acc_names};"
            if not inSet([bnd_name], bnd_names):
                rnt += f"Bond {bnd_name} is not in deal bonds {bnd_names};"
        
        elif action.get("tag") == 'CalcFee' and len(action.get("contents", [])) == 1:
            fs = action["contents"]
            if not inSet(fs, fee_names):
                rnt += f"Fee {fs} is not in deal fees {fee_names};"
        
        elif action.get("tag") == 'CalcBondInt' and len(action.get("contents", [])) == 1:
            bs = action["contents"]
            if not inSet(bs, bnd_names):
                rnt += f"Bond {bs} is not in deal bonds {bnd_names};"
        
        elif action.get("tag") == 'SwapSettle' and len(action.get("contents", [])) == 2:
            acc, swap_name = action["contents"]
            if not inSet([acc], acc_names):
                rnt += f"Account: {acc} is not in deal accounts {acc_names};"
            if not inSet([swap_name], swap_names):
                rnt += f"Swap: {swap_name} is not in deal swap list {swap_names};"
        
        return rnt

    for wn, waterfallActions in w.items():
        for idx, action in enumerate(waterfallActions):
            if (vr := validateAction(action)) != "":
                error.append(">".join((wn, str(idx), vr)))
    
    # if preclosing deal, must have a preClosing status
    if d['dates']['tag'] == 'PreClosingDates':
        if d['status']['tag'] != 'PreClosing':
            error.append(f"Deal Date is preClosing, but status is not PreClosing")

    return (error, warning)


def valReq(reqSent) -> list:
    error = []
    warning = []
    req = json.loads(reqSent)

    if isinstance(req, dict) and req.get("tag") == "SingleRunReq":
        contents = req.get("contents", [])
        if contents and len(contents) >= 3:
            d, ma, mra = contents[0]["contents"], contents[1], contents[2]
            error, warning = valDeal(d, error, warning)
            error, warning = valAssumption(d, ma, error, warning)
            error, warning = valNonPerfAssumption(d, mra, error, warning)
        else:
            raise RuntimeError(f"Invalid structure for SingleRunReq: {req}")

    elif isinstance(req, dict) and req.get("tag") == "MultiScenarioRunReq":
        contents = req.get("contents", [])
        if contents and len(contents) >= 3:
            d, mam, mra = contents[0]["contents"], contents[1], contents[2]
            error, warning = valDeal(d, error, warning)
        else:
            raise RuntimeError(f"Invalid structure for MultiScenarioRunReq: {req}")

    elif isinstance(req, dict) and req.get("tag") == "MultiDealRunReq":
        contents = req.get("contents", [])
        if contents and len(contents) >= 3:
            dm, ma, mra = contents[0], contents[1], contents[2]
            error, warning = valAssumption(ma, error, warning)
            error, warning = valNonPerfAssumption(dm, ma, error, warning)
        else:
            raise RuntimeError(f"Invalid structure for MultiDealRunReq: {req}")

    else:
        raise RuntimeError(f"Failed to match request: {req}")

    return error, warning



def valNonPerfAssumption(d, nonPerfAssump, error, warning) -> list:
    e = []
    w = []
    # floater index required
    indexRequired = set()
    ## from bond 
    #bndIndx = set([ _['contents'][1] for _ in  query(d,['bonds',S.MVALS,S.ALL,'bndInterestInfo']) if _['tag']=='Floater'])
    ## from asset
    ## from account
    ## from ir swap
    indexSupplied = set([ _['contents'][0] for _ in nonPerfAssump['interest'] ]) if 'interest' in nonPerfAssump else set()
    assert isinstance(indexSupplied,set),f"indexSupplied should be set but got type {type(indexSupplied)}  value: {indexSupplied}"
    if not indexSupplied.issuperset(indexRequired):
        e.append(f"Missing floater index:{indexRequired - indexSupplied}")
    return error+e,warning+w



def valAssumption(d, ma, error, warning) -> list:
    def _validate_single_assump(z):
        if isinstance(z, dict) and z.get('tag') == 'PoolLevel':
            return [], []
        elif isinstance(z, dict) and z.get('tag') == 'ByIndex' and isinstance(z.get('contents'), list):
            assumps, _ = z['contents']
            _e = []
            _w = []
            _ids = set(flat([assump[0] for assump in assumps]))
            if not _ids.issubset(asset_ids):
                _e.append(f"Not Valid Asset ID:{_ids - asset_ids}")
            if len(missing_asset_id := asset_ids - _ids) > 0:
                _w.append(f"Missing Asset to set assumption:{missing_asset_id}")
            return _e, _w
        else:
            raise RuntimeError(f"Failed to match:{z}")

    #asset_ids = set(range(len(list(query(d, ['pool', 'assets'])))))
    if ma is None:
        return error, warning
    else:
        e, w = _validate_single_assump(ma)
        return error + e, warning + w
