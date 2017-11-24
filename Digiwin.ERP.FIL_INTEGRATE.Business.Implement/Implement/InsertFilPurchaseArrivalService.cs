//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-03-24</createDate>
//<description>依送货单生成到货单服务</description>
//---------------------------------------------------------------- 
//20170508 modi by liwei1 for P001-161209002
//20170619 modi by zhangcn for P001-170606002
//20170630 modi by zhangcn for B001-170629006 更新送货单单身的状态修改
//20170711 add by zhangcn for B002-170710028 生成到货单IInsertPurchaseArrivalService，金额字段与订单有点尾差
//20170919 modi by liwei1 for B001-170918011 商品类型为赠备品时，计价数量为0

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using Digiwin.ERP.CommonSupplyChain.Business;
using Digiwin.ERP.EFNET.Business;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    

    /// <summary>
    /// 依送货单生成到货单服务
    /// </summary>
    [ServiceClass(typeof(IInsertFilPurchaseArrivalService))]
    [Description("依送货单生成到货单服务")]
    public class InsertFilPurchaseArrivalService : ServiceComponent, IInsertFilPurchaseArrivalService {
        #region 全局变量
        IDataEntityType _tableScan;//参数临时表
        IDataEntityType _purchaseArrivalD;
        IQueryService _querySrv;
        private IBudgetService _budgetSrv; //预算公共服务 //20170619 add by zhangcn for P001-170606002
        #endregion

        /// <summary>
        /// 产生到货单信息
        /// </summary>
        /// <param name="receipt_detail">receipt_detail集合包括：送货单号（delivery_no）、品号（item_no）、特征码（item_feature_no）、数量（qty）</param>
        /// <returns>单据编号</returns>
        public Hashtable InsertFilPurchaseArrival(DependencyObjectCollection receipt_detail) {
            try {
                IInfoEncodeContainer infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务 
                _budgetSrv = GetServiceForThisTypeKey<IBudgetService>(); //20170619 add by zhangcn for P001-170606002
                UtilsClass utilsClass = new UtilsClass();
                DependencyObjectCollection resultColl = utilsClass.CreateReturnCollection();
                using (IConnectionService connectionService = GetService<IConnectionService>()) {
                    _querySrv = this.GetService<IQueryService>();
                    //参数检查+参数存入临时表+入参数量存入字典并返回
                    Dictionary<string, decimal> qtyDictionary = ParameterCheck(receipt_detail, infoCodeSer);
                    using (ITransactionService transActionService = GetService<ITransactionService>()) {
                        //插入到货单
                        InsertPurchaseArival(resultColl, qtyDictionary, infoCodeSer);
                        transActionService.Complete();
                    }
                }
                //组织返回结果
                Hashtable result = new Hashtable{{"doc_no", resultColl}};
                return result;
            } catch (System.Exception) {
                throw;
            }
        }

        /// <summary>
        /// 参数检查+参数存入临时表
        /// </summary>
        /// <param name="receiptDetail"></param>
        /// <param name="infoCodeSer"></param>
        /// <returns>入参数量存入字典</returns>
        private Dictionary<string, decimal> ParameterCheck(DependencyObjectCollection receiptDetail, IInfoEncodeContainer infoCodeSer) {
            //子查询中不能用CreateDyncParameter方法，只能拼接In条件里面对应数据
            List<ConstantsQueryProperty> docNo = new List<ConstantsQueryProperty>();
            List<ConstantsQueryProperty> itemCode = new List<ConstantsQueryProperty>();
            List<ConstantsQueryProperty> itenFeatuer = new List<ConstantsQueryProperty>();
            Dictionary<string, decimal> qtyDictionary = new Dictionary<string, decimal>();//入参数量存储字典
            #region 参数检查
            foreach (var item in receiptDetail) {
                if (Maths.IsEmpty(item["delivery_no"])) {
                    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "delivery_no"));//‘入参【delivery_no】未传值’
                }
                if (Maths.IsEmpty(item["item_no"])) {
                    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "item_no"));//‘入参【item_no】未传值’
                }
                if (Maths.IsEmpty(item["qty"])) {
                    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "qty"));//‘入参【qty】未传值’
                }
                docNo.Add(OOQL.CreateConstants(item["delivery_no"].ToStringExtension()));
                itemCode.Add(OOQL.CreateConstants(item["item_no"].ToStringExtension()));
                itenFeatuer.Add(OOQL.CreateConstants(item["item_feature_no"].ToStringExtension()));

                //将数量存入字典，用于后面数量拆分成生成到货单单身业务数量
                string key = item["delivery_no"].ToStringExtension() + item["item_no"].ToStringExtension() + item["item_feature_no"].ToStringExtension();
                if (!qtyDictionary.ContainsKey(key)) {
                    qtyDictionary.Add(key, item["qty"].ToDecimal());
                }
            }
            #endregion
            DataTable tableScan = CreateDtForTableScanBulk();
            DependencyObjectCollection paraDocFil = QueryParaDocFil(docNo, itemCode, itenFeatuer);
            List<object> purchaseOrderId = new List<object>();//记录取得前置单据单据性质的采购订单主键
            if (paraDocFil.Count > 0) {
                foreach (var item in paraDocFil) {
                    //来源单据单据性质不为空
                    if (!Maths.IsEmpty(item["SOURCE_DOC_ID"])) {
                        purchaseOrderId.Add(item["ORDER_NO"]);
                    } else {
                        //该采购订单如果是取前置单据，来源单据为空跳出循环（去除重复数据）
                        if (purchaseOrderId.Contains(item["ORDER_NO"])) {
                            continue;
                        }
                    }
                    DataRow drNewDetail = tableScan.NewRow();
                    drNewDetail["DELIVERTY_NO"] = item["DOC_NO"];
                    drNewDetail["ITEM_CODE"] = item["ITEM_CODE"];
                    drNewDetail["ITEM_FEATURE_NO"] = item["ITEM_FEATURE_CODE"];
                    drNewDetail["DOC_ID"] = item["DOC_ID"];
                    drNewDetail["CATEGORY"] = item["CATEGORY"];
                    drNewDetail["ORDER_NO"] = item["ORDER_NO"];
                    tableScan.Rows.Add(drNewDetail);
                }
            } else {
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111275"));//未找到对应的FIL单据类型设置，请检查！
            }
            //创建参数存储临时表
            CreateTableScanTempTable();
            //将参数插入临时表中
            CreateTableScan(tableScan);
            return qtyDictionary;
        }

        /// <summary>
        /// 生成到货单
        /// </summary>
        private void InsertPurchaseArival(DependencyObjectCollection resultColl, Dictionary<string, decimal> qtyDictionary, IInfoEncodeContainer infoCodeSer) {
            //按照也许需求对查询结果排序，用于计算单身业务数量和所需生成单头笔数
            DataTable purchaseArival= QueryForPurchaseArrival();
            if (purchaseArival.Rows.Count > 0) {
                //生成主键服务 
                IPrimaryKeyService keyService = GetService<IPrimaryKeyService>(TypeKey);
                string addPurchaseArrivalKeyTemp = string.Empty;//用于暂存生成到货单单头需按照分组的字段
                DataTable addPurchaseArival = purchaseArival.Clone();//复制表结构
                DataTable busQtyDataTable = CreateDtForPurchaseArrivalDBulk();//记录单身业务数量、单头主键、采购订单子单身主键
                object purchaseArrivalId = Maths.GuidDefaultValue();//到货单单头主键
                Dictionary<string, decimal> qtyTempDictionary = new Dictionary<string, decimal>();//生成不同到货单的时候，第二张到货单对应数量应该是前面这张到货单扣除之后的数量去赋值
                string deliveryNo = purchaseArival.Rows[0]["DELIVERTY_NO"].ToStringExtension();//20170508 add by liwei1 for P001-161209002
                foreach (DataRow item in purchaseArival.Rows){
                    //根据采购域，供应商，含税，结算公司，币种，税种，单据性质分组生成到货单
                    string addPurchaseArrivalKey = item["Owner_Org_ROid"].ToStringExtension()
                                                        + item["SUPPLIER_ID"].ToStringExtension()
                                                        + item["TAX_INCLUDED"].ToStringExtension()
                                                        + item["INVOICE_COMPANY_ID"].ToStringExtension()
                                                        + item["CURRENCY_ID"].ToStringExtension()
                                                        + item["DOC_ID"].ToStringExtension()
                                                        + item["D_TAX_ID"].ToStringExtension();
                    if (addPurchaseArrivalKey != addPurchaseArrivalKeyTemp) {
                        purchaseArrivalId = item["PURCHASE_ARRIVAL_ID"] = keyService.CreateId();//生成单头主键
                        addPurchaseArival.Rows.Add(item.ItemArray);//降满足生成到货单行插入预生成到货单DataTable
                        addPurchaseArrivalKeyTemp = addPurchaseArrivalKey;//更新循环外暂存分组Key.05

                    } else {
                        item["PURCHASE_ARRIVAL_ID"] = purchaseArrivalId;//更新到货单主键
                    }

                    decimal busQty = 0m;
                    decimal qty = 0m;//入参数量qty
                    decimal qtyTemp = 0m;//同一组数量扣除后缓存
                    string key = item["DELIVERTY_NO"].ToStringExtension() + item["ITEM_CODE"].ToStringExtension() + item["ITEM_FEATURE_CODE"].ToStringExtension();
                    //当单号、品号、特征码和循环外记录的keyTemp主键缓存不相同时，说明同一组数量已经拆分完成。重新获取下一笔数量进行计算。
                    if (qtyDictionary.ContainsKey(key)) {
                        //传入参数每一组只取一次数量并更新主键缓存
                        qty = qtyDictionary[key];
                        if (!qtyTempDictionary.ContainsKey(key)) {
                            qtyTempDictionary.Add(key,qty);
                            qtyTemp = qty;
                        } else {
                            qtyTemp = qtyTempDictionary[key];
                        }
                    }

                    //未交量(QUERYQTY.SUM_UNARR_QTY) +总允许超交数量 QUERYQTY.SUM_OVER_QTY<【入参数量qty】
                    if ((item["SUM_UNARR_QTY"].ToDecimal() + item["SUM_OVER_QTY"].ToDecimal()) < qty) {
                        throw new BusinessRuleException(infoCodeSer.GetMessage("A111377"));//E+MSG:” 生成到货单的数量不能超过未交量和允许超交数量总和！（A111377）”
                    }
                    if (qtyTemp > 0){
                        //剩余入参数量必须大于0才计算业务数量
                        busQty = item["ACTUAL_QTY"].ToDecimal() <= qtyTemp ? item["ACTUAL_QTY"].ToDecimal() : qtyTemp;
                        qtyTemp = qtyTemp - busQty;
                    }
                    //更新缓存入参数量
                    if (qtyTempDictionary.ContainsKey(key)) {
                        qtyTempDictionary[key] = qtyTemp;
                    }
                    //可分配业务数量小于等于0的情况下删除当前明细，生成业务数量为0到货单无意义。便不记录
                    if (busQty > 0) {
                        DataRow drNewDetail = busQtyDataTable.NewRow();
                        drNewDetail["BUS_QTY"] = busQty;
                        drNewDetail["PURCHASE_ORDER_SD_ID"] = item["PURCHASE_ORDER_SD_ID"];
                        drNewDetail["PURCHASE_ARRIVAL_ID"] = item["PURCHASE_ARRIVAL_ID"];
                        drNewDetail["DOC_ID"] = item["DOC_ID"];
                        busQtyDataTable.Rows.Add(drNewDetail);
                    }

                    //20170619 add by zhangcn for P001-170606002 ===beigin===
                    DependencyObjectCollection collSupplySyneryFiD = 
                        QuerySupplySyneryFiD(item["DELIVERTY_NO"], item["ITEM_CODE"], item["ORDER_NO"], item["ITEM_FEATURE_NO"]);
                    if (collSupplySyneryFiD.Count > 0) {
                        item["GROUP_SYNERGY_D_ID"] = collSupplySyneryFiD[0]["SUPPLY_SYNERGY_FI_D_ID"];
                    }
                    //20170619 add by zhangcn for P001-170606002 ===end===
                }
                //创建参数存储临时表
                CreatePurchaseArrivalDTempTable();
                //将参数插入临时表中
                List<BulkCopyColumnMapping> dtScanMapping = GetBulkCopyColumnMapping(busQtyDataTable.Columns);
                _querySrv.BulkCopy(busQtyDataTable, _purchaseArrivalD.Name, dtScanMapping.ToArray());

                //到货单实体列（删除计算列用到的字段）
                DataColumnCollection paColumnsCollection=RemovePurchaseArivalColumns(addPurchaseArival);

                if (addPurchaseArival.Rows.Count > 0) {
                    ICreateService createSrv = GetService<ICreateService>("PURCHASE_ARRIVAL");
                    DependencyObject entity = createSrv.Create() as DependencyObject;
                    ISaveService saveService = GetService<ISaveService>("PURCHASE_ARRIVAL");//保存服务
                    IEFNETStatusStatusService efnetSrv = GetService<IEFNETStatusStatusService>();//自动签核
                    ITaxesService taxService = GetServiceForThisTypeKey<ITaxesService>();
                    ICurrencyPrecisionService currencyPrecisionSrv = GetServiceForThisTypeKey<ICurrencyPrecisionService>();//20170711 add by zhangcn for B002-170710028
                    IItemQtyConversionService itemQtyConversionService = GetServiceForThisTypeKey<IItemQtyConversionService>();
                    //单号服务
                    IDocumentNumberGenerateService docNumberService = GetService<IDocumentNumberGenerateService>("PURCHASE_ARRIVAL");

                    //查询预生成到货单单身数据
                    DataTable purchaseArivalD = QueryForPurchaseArricalD();
                    #region 计算列 20170619 add by zhangcn for P001-170606002

                    DateTime dtArrivalDate = addPurchaseArival.Rows[0]["ARRIVAL_DATE"].ToDate();
                    foreach (DataRow dr_d in purchaseArivalD.Rows) {
                        if (Maths.IsNotEmpty(dr_d["BUDGET_ADMIN_UNIT_ID"])) {
                            object[] budgetArr = _budgetSrv.GetPerformanceBudget(dtArrivalDate, dr_d["BUDGET_GROUP_ID"], dr_d["BUDGET_ITEM_ID"],
                                                                                dr_d["BUDGET_ADMIN_UNIT_ID"], dr_d["PRE_BUDGET_ID"], dr_d["PRE_BUDGET_D_ID"]);

                            dr_d["BUDGET_ID"] = budgetArr[0];
                            dr_d["BUDGET_D_ID"] = budgetArr[1];
                        }
                    }
                    #endregion

                    //生成到货单单身字段列
                    DataColumnCollection padColumnsCollection = purchaseArivalD.Clone().Columns;//复制表结构中的列名
                    //删除多余计算列
                    padColumnsCollection.Remove("STOCK_UNIT_ID");
                    padColumnsCollection.Remove("INSPECT_MODE");
                    padColumnsCollection.Remove("QC_RESULT_INPUT_TYPE");
                    //根据单头主键分组
                    List<IGrouping<object, DataRow>> groupDt = purchaseArivalD.AsEnumerable().GroupBy(a => (a.Field<object>("PURCHASE_ARRIVAL_ID"))).ToList();
                    List<QueryProperty> purchaseArivaldList = new List<QueryProperty>();//20170508 add by liwei1 for P001-161209002
                    foreach (DataRow dr in addPurchaseArival.Rows) {
                        //创建单头实体
                        if (entity != null){
                            DependencyObject newEntity = new DependencyObject(entity.DependencyObjectType);
                            DependencyObjectCollection newEntityDColl = newEntity["PURCHASE_ARRIVAL_D"] as DependencyObjectCollection;
                            dr["DOC_NO"] = docNumberService.NextNumber(dr["DOC_ID"], dr["DOC_DATE"].ToDate().Date);//生成单头单号
                            //给实体填入对应数据
                            AddToEntity(newEntity, dr, paColumnsCollection, false);
                            List<IGrouping<object, DataRow>> entityDColl = groupDt.Where(c => c.Key.Equals(dr["PURCHASE_ARRIVAL_ID"])).ToList();
                            decimal[] taxResult = { 0M, 0M, 0M, 0M };//暂存单头合计相关字段数据
                            int sequenceNumber = 1;//单身初始序号
                            foreach (IGrouping<object, DataRow> groupDColl in entityDColl) {
                                foreach (DataRow drPaD in groupDColl) {
                                    //在单头实体中创建单身实体
                                    if (newEntityDColl != null){
                                        DependencyObject newEntityD = new DependencyObject(newEntityDColl.ItemDependencyObjectType);
                                        //更新单身当前比相关数据
                                        UpdatePurchaseArrivallD(dr, drPaD, sequenceNumber, keyService, taxService, itemQtyConversionService, currencyPrecisionSrv);//20170711 modi by zhangcn for B002-170710028 增加传参 currencyPrecisionSrv
                                        //给实体填入对应数据
                                        AddToEntity(newEntityD, drPaD, padColumnsCollection, true);
                                        //汇总单头相关合计字段数据
                                        newEntityDColl.Add(newEntityD);
                                    }
                                    taxResult[0] += drPaD["AMOUNT_UNINCLUDE_TAX_OC"].ToDecimal();
                                    taxResult[1] += drPaD["TAX_OC"].ToDecimal();
                                    taxResult[2] += drPaD["AMOUNT_UNINCLUDE_TAX_BC"].ToDecimal();
                                    taxResult[3] += drPaD["TAX_BC"].ToDecimal();
                                    sequenceNumber++;//序号自增1
                                }
                            }
                            //单身汇总
                            newEntity["AMOUNT_UNINCLUDE_TAX_OC"] = taxResult[0];
                            newEntity["TAX_OC"] = taxResult[1];
                            newEntity["AMOUNT_UNINCLUDE_TAX_BC"] = taxResult[2];
                            newEntity["TAX_BC"] = taxResult[3];

                            saveService.Save(newEntity);//希望触发保存校验
                            //7.3自动签核
                            efnetSrv.GetFormFlow("PURCHASE_ARRIVAL.I01", dr["DOC_ID"], dr["Owner_Org_ROid"],
                                new List<object>{ dr["PURCHASE_ARRIVAL_ID"] });

                            //生成成功单号添加至返回结果集resultColl中
                            DependencyObject resultObj = resultColl.AddNew();
                            resultObj["doc_no"] = newEntity["DOC_NO"];
                            purchaseArivaldList.Add(OOQL.CreateConstants(newEntity["PURCHASE_ARRIVAL_ID"]));//20170508 add by liwei1 for P001-161209002
                        }
                    }
                    //20170508 add by liwei1 for P001-161209002 ---begin---
                    //更新送货单单身状态码
                    UpdateFilArrivalD(deliveryNo, purchaseArivaldList);
                    // 送货单单身所有笔数的的状态码均为‘Y.已收货’则将单头的状态码也更新成‘3.已收货’
                    if (StatusIsOk(deliveryNo)){
                        UpdateFilArrival(deliveryNo);
                    }
                    //20170508 add by liwei1 for P001-161209002 ---end---
                }
            }
        }

        #region //20170508 add by liwei1 for P001-161209002

        /// <summary>
        /// 更新销货单单身状态码
        /// </summary>
        /// <param name="deliveryNo"></param>
        /// <param name="purchaseArivaldList"></param>
        private void UpdateFilArrivalD(string deliveryNo, List<QueryProperty> purchaseArivaldList) {
            QueryNode node =
                OOQL.Select(OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID"),   
                    OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID"), 
                    OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO"),                     
                    OOQL.CreateProperty("FIL_ARRIVAL_D.SequenceNumber"))
                .From("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                .InnerJoin(
                    OOQL.Select(OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID"), 
                    OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO"),                   
                    OOQL.CreateProperty("FIL_ARRIVAL_D.SequenceNumber"))
                    .From("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                    .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO")))
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")) 
                        & (OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE")))
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")) 
                        & (OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE")))
                    .Where((OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateConstants(deliveryNo))), "FIL_ARRIVAL_D")
                .On((OOQL.CreateProperty("FIL_ARRIVAL_D.PURCHASE_ORDER_SD_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SOURCE_ID.ROid")))
                .Where(OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID").In(purchaseArivaldList.ToArray()));

            node = OOQL.Update("FIL_ARRIVAL.FIL_ARRIVAL_D")
                .Set(new[]{
                    new SetItem(OOQL.CreateProperty("STATUS"), OOQL.CreateConstants("Y")), //20170630 modi by zhangcn for B001-170629006【OLD：3】
                    new SetItem(OOQL.CreateProperty("PURCHASE_ARRIVAL_ID"), OOQL.CreateProperty("A.PURCHASE_ARRIVAL_ID")),
                    new SetItem(OOQL.CreateProperty("PURCHASE_ARRIVAL_D_ID"),OOQL.CreateProperty("A.PURCHASE_ARRIVAL_D_ID"))
                })
                .From(node, "A")
                .Where((OOQL.CreateProperty("A.DOC_NO") ==OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.DOC_NO"))
                       &(OOQL.CreateProperty("A.SequenceNumber") ==OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.SequenceNumber")));
            UtilsClass.ExecuteNoQuery(GetService<IQueryService>(), node, false);
        }
        

        /// <summary>
        /// 更新销货单单头状态码
        /// </summary>
        /// <param name="deliveryNo"></param>
        private void UpdateFilArrival(string deliveryNo) {
            QueryNode updateNode = OOQL.Update("FIL_ARRIVAL", new[]{
                    new SetItem(OOQL.CreateProperty("FIL_ARRIVAL.STATUS"),OOQL.CreateConstants("3"))
                })
                .Where(OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO") == OOQL.CreateConstants(deliveryNo));
            GetService<IQueryService>().ExecuteNoQueryWithManageProperties(updateNode);
        }
        

        /// <summary>
        /// 送货单单身所有笔数的的状态码均为‘3.已收货’返回true
        /// </summary>
        /// <param name="deliveryNo"></param>
        /// <returns></returns>
        private bool StatusIsOk(string deliveryNo){
            bool isOk = true;
            QueryNode node =
                OOQL.Select(
                            Formulas.Count(
                                OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.FIL_ARRIVAL_D_ID"), "COUNT_NUM"))
                        .From("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                        .Where((OOQL.AuthFilter("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D"))
                               & ((OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.STATUS") != OOQL.CreateConstants("Y"))//20170630 modi by zhangcn for B001-170629006【OLD：3】
                               & (OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.DOC_NO") ==OOQL.CreateConstants(deliveryNo))));
            //查询单身存在状态码为“3.已收货”的记录数返回FALSE，不更新单头
            if (GetService<IQueryService>().ExecuteScalar(node).ToInt32() > 0) {
                isOk = false;
            }
            return isOk;
        }

        #endregion

        /// <summary>
        /// 更新生成到货单子单身计算列数据
        /// </summary>
        /// <param name="dr"></param>
        /// <param name="drPaD"></param>
        /// <param name="sequenceNumber"></param>
        /// <param name="keyService"></param>
        /// <param name="taxService"></param>
        /// <param name="itemQtyConversionService"></param>
        private void UpdatePurchaseArrivallD(DataRow dr, DataRow drPaD, int sequenceNumber, IPrimaryKeyService keyService, ITaxesService taxService, IItemQtyConversionService itemQtyConversionService, ICurrencyPrecisionService currencyPrecisionSrv) {//20170711 modi by zhangcn for B002-170710028 增加传参 currencyPrecisionSrv
            drPaD["PURCHASE_ARRIVAL_D_ID"] = keyService.CreateId();//生成单身主键
            drPaD["SequenceNumber"] = sequenceNumber;//序号
            string inspectMode = drPaD["INSPECT_MODE"].ToStringExtension();
            //20170919 add by liwei1 for B001-170918011 ===begin===
            if(drPaD["ITEM_TYPE"].ToStringExtension() != "1"){
                drPaD["PRICE_QTY"] = 0m;
            } else{
                //20170919 add by liwei1 for B001-170918011 ===end===
                //允收业务数量
                if(inspectMode == "1" || (inspectMode == "3" && drPaD["QC_RESULT_INPUT_TYPE"].ToStringExtension() == "1")){
                    drPaD["ACCEPTED_BUSINESS_QTY"] = drPaD["BUSINESS_QTY"];
                    //计价数量	
                    if(!(drPaD["BUSINESS_UNIT_ID"].Equals(drPaD["PRICE_UNIT_ID"]))){
                        drPaD["PRICE_QTY"] = itemQtyConversionService.GetConvertedQty(
                                                    drPaD["ITEM_ID"],
                                                    drPaD["BUSINESS_UNIT_ID"],
                                                    drPaD["BUSINESS_QTY"].ToDecimal(),
                                                    drPaD["PRICE_UNIT_ID"]);
                    } else{
                        drPaD["PRICE_QTY"] = drPaD["BUSINESS_QTY"].ToDecimal();
                    }
                }
            }//20170919 add by liwei1 for B001-170918011

            //允收库存单位数量
            drPaD["ACCEPTED_INVENTORY_QTY"] = itemQtyConversionService.GetConvertedQty(
                                                    drPaD["ITEM_ID"],
                                                    drPaD["BUSINESS_UNIT_ID"],
                                                    drPaD["ACCEPTED_BUSINESS_QTY"].ToDecimal(),
                                                    drPaD["STOCK_UNIT_ID"]);

            //合格业务数量
            if (inspectMode == "3" && drPaD["QC_RESULT_INPUT_TYPE"].ToStringExtension() == "1") {
                drPaD["QUALIFIED_BUSINESS_QTY"] = drPaD["BUSINESS_QTY"];
            }
            
            //应结算计价数量	
            drPaD["SHOULD_SETTLE_PRICE_QTY"] = drPaD["PRICE_QTY"];
            //折扣额		(到货单单身 单价(PRICE)- 到货单单身折扣后单价(DISCOUNTED_PRICE))*到货单单身计价数量(PRICE_QTY)
            drPaD["DISCOUNT_AMT"] = (drPaD["PRICE"].ToDecimal() - drPaD["DISCOUNTED_PRICE"].ToDecimal()) * drPaD["PRICE_QTY"].ToDecimal();
            //金额		到货单单身计价数量(PRICE_QTY)*到货单单身折扣后单价(DISCOUNTED_PRICE)
            drPaD["AMOUNT"] = drPaD["PRICE_QTY"].ToDecimal() * drPaD["DISCOUNTED_PRICE"].ToDecimal();
            decimal amount = currencyPrecisionSrv.AmendAmountPrecision(dr["CURRENCY_ID"], drPaD["AMOUNT"].ToDecimal()).ToDecimal(0);//20170711 add by zhangcn for B002-170710028
            decimal[] taxResult = taxService.GetTaxes(dr["CURRENCY_ID"],
                                dr["INVOICE_COMPANY_ID"],
                                dr["EXCHANGE_RATE"].ToDecimal(),
                                drPaD["TAX_ID"],
                                drPaD["TAX_RATE"].ToDecimal(),
                                dr["TAX_INCLUDED"].ToBoolean(),
                                amount);//20170711 add by zhangcn for B002-170710028 OLD:drPaD["AMOUNT"].ToDecimal());
            drPaD["AMOUNT_UNINCLUDE_TAX_OC"] = taxResult[0];
            drPaD["TAX_OC"] = taxResult[1];
            drPaD["AMOUNT_UNINCLUDE_TAX_BC"] = taxResult[2];
            drPaD["TAX_BC"] = taxResult[3];

            //检验状态INSPECTION_STATUS
            if (inspectMode == "1") {
                drPaD["INSPECTION_STATUS"] = "1";//1.免检
            } else if (inspectMode == "2" || (inspectMode == "3" && drPaD["QC_RESULT_INPUT_TYPE"].ToStringExtension() == "2")) {
                drPaD["INSPECTION_STATUS"] = "2";//2.待验
            } else if (inspectMode == "3" && drPaD["QC_RESULT_INPUT_TYPE"].ToStringExtension() == "1") {
                drPaD["INSPECTION_STATUS"] = "4";//4.检验完成
                //已判定业务数量= 默认[允收业务数量]ACCEPTED_BUSINESS_QTY+[拒收业务数量]RETURN_BUSINESS_QTY+[特采业务数量]SP_RECEIPT_BUSINESS_QTY+[报废业务数量]SCRAP_BUSINESS_QTY
                drPaD["JUDGED_QTY"] = drPaD["ACCEPTED_BUSINESS_QTY"].ToDecimal()
                    + drPaD["RETURN_BUSINESS_QTY"].ToDecimal()
                    + drPaD["SP_RECEIPT_BUSINESS_QTY"].ToDecimal()
                    + drPaD["SCRAP_BUSINESS_QTY"].ToDecimal();
                //已检验业务数量= 默认[不合格业务数量]UNQUALIFIED_BUSINESS_QTY+[检验破坏量]IN_DESTROYED_BUSINESS_QTY+[合格业务数量]QUALIFIED_BUSINESS_QTY 
                drPaD["INSPECTED_QTY"] = drPaD["UNQUALIFIED_BUSINESS_QTY"].ToDecimal()
                    + drPaD["IN_DESTROYED_BUSINESS_QTY"].ToDecimal()
                    + drPaD["QUALIFIED_BUSINESS_QTY"].ToDecimal();
            }
        }

        /// <summary>
        /// 删除生成到货单多余列
        /// </summary>
        /// <param name="addPurchaseArival"></param>
        private DataColumnCollection RemovePurchaseArivalColumns(DataTable addPurchaseArival) {
            DataColumnCollection paColumnsCollection = addPurchaseArival.Columns;
            paColumnsCollection.Remove("DELIVERTY_NO");
            paColumnsCollection.Remove("ITEM_CODE");
            paColumnsCollection.Remove("ITEM_FEATURE_CODE");
            paColumnsCollection.Remove("ORDER_NO");
            paColumnsCollection.Remove("ORDER_SE");
            paColumnsCollection.Remove("ORDER_SE_SE");
            paColumnsCollection.Remove("UNARR_QTY");
            paColumnsCollection.Remove("ACTUAL_QTY");
            paColumnsCollection.Remove("OVER_QTY");
            paColumnsCollection.Remove("SUM_UNARR_QTY");
            paColumnsCollection.Remove("SUM_OVER_QTY");
            paColumnsCollection.Remove("PLAN_ARRIVAL_DATE");
            paColumnsCollection.Remove("PURCHASE_ORDER_SD_ID");
            paColumnsCollection.Remove("D_TAX_ID");
            return paColumnsCollection;
        }

        /// <summary>
        /// 实体赋值
        /// </summary>
        /// <param name="targetObj"></param>
        /// <param name="dr"></param>
        /// <param name="dcColl"></param>
        /// <param name="isIgnorePaId"></param>
        private void AddToEntity(DependencyObject targetObj, DataRow dr, DataColumnCollection dcColl, bool isIgnorePaId) {
            foreach (DataColumn dc in dcColl) {
                if (dc.ColumnName.StartsWith("temp")) continue;//单头多查询了这类的字段做后续计算用
                string targetName = dc.ColumnName;//列名
                //列名中的下划线大于0，且以[_RTK]或[_ROid]结尾的列名视为多来源字段
                if ((targetName.IndexOf("_", StringComparison.Ordinal) > 0)
                    && (targetName.EndsWith("_RTK", StringComparison.CurrentCultureIgnoreCase)
                        || targetName.EndsWith("_ROid", StringComparison.CurrentCultureIgnoreCase))) {
                    //最后一个下划线后一位位置
                    var endPos = targetName.LastIndexOf("_", StringComparison.Ordinal) + 1;
                    //拼接目标字段名
                    string firstName = targetName.Substring(0, endPos - 1);
                    string endName = targetName.Substring(endPos, targetName.Length - endPos);
                    ((DependencyObject)targetObj[firstName])[endName] = dr[dc.ColumnName];
                } else {
                    if (!isIgnorePaId || dc.ColumnName != "PURCHASE_ARRIVAL_ID") {
                        targetObj[dc.ColumnName] = dr[dc.ColumnName];
                    }
                }
            }
        }

        private DataTable QueryForPurchaseArricalD() {
            object idDefaultValue = Maths.GuidDefaultValue();
            const decimal decimalDefaultValue = 0m;
            QueryNode node = OOQL.Select(
                                                        OOQL.CreateProperty("ITEM.STOCK_UNIT_ID", "STOCK_UNIT_ID"),
                                                        Formulas.Case(null,
                                                                Formulas.IsNull(
                                                                        OOQL.CreateProperty("ITEM_PLANT.INSPECT_MODE"),
                                                                        OOQL.CreateConstants(string.Empty)),
                                                                OOQL.CreateCaseArray(
                                                                        OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_TYPE") == OOQL.CreateConstants("3")),
                                                                                Formulas.IsNull(
                                                                                    OOQL.CreateProperty("MO_ROUTING_D.INSPECT_MODE"),
                                                                                    OOQL.CreateConstants(string.Empty)))), "INSPECT_MODE"),
                                                        OOQL.CreateProperty("DOC.QC_RESULT_INPUT_TYPE", "QC_RESULT_INPUT_TYPE"),

                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "PURCHASE_ARRIVAL_D_ID"),
                                                        OOQL.CreateProperty("A.PURCHASE_ARRIVAL_ID", "PURCHASE_ARRIVAL_ID"),
                                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String, "SequenceNumber"),
                                                        OOQL.CreateProperty("ITEM.ITEM_ID", "ITEM_ID"),
                                                        OOQL.CreateProperty("ITEM.ITEM_NAME", "ITEM_DESCRIPTION"),
                                                        Formulas.IsNull(
                                                                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                                                                    OOQL.CreateConstants(idDefaultValue), "ITEM_FEATURE_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_SPECIFICATION", "ITEM_SPECIFICATION"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_TYPE", "ITEM_TYPE"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "PACKING_MODE_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "PACKING_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "PACKING1_UNIT_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "PACKING2_UNIT_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "PACKING3_UNIT_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "PACKING4_UNIT_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID", "BUSINESS_UNIT_ID"),
                                                        OOQL.CreateProperty("A.BUS_QTY", "BUSINESS_QTY"),
                                                        Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                                    OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID"),
                                                                    OOQL.CreateProperty ("A.BUS_QTY"),
                                                                    OOQL.CreateProperty ("ITEM.STOCK_UNIT_ID"),
                                                                    OOQL.CreateConstants(0)
                                                        }),
                                                        Formulas.Ext("UNIT_CONVERT", "SECOND_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                                    OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID"),
                                                                    OOQL.CreateProperty ("A.BUS_QTY"),
                                                                    OOQL.CreateProperty ("ITEM.SECOND_UNIT_ID"),
                                                                    OOQL.CreateConstants(0)
                                                         }),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.PRICE_UNIT_ID", "PRICE_UNIT_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "PRICE_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "SHOULD_SETTLE_PRICE_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "PACKING_QTY1"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "PACKING_QTY2"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "PACKING_QTY3"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "PACKING_QTY4"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.PRICE", "PRICE"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.DISCOUNT_RATE", "DISCOUNT_RATE"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.DISCOUNTED_PRICE", "DISCOUNTED_PRICE"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "DISCOUNT_AMT"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.STANDARD_PRICE", "STANDARD_PRICE"),
                                                        OOQL.CreateConstants("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", GeneralDBType.String, "SOURCE_ID_RTK"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID", "SOURCE_ID_ROid"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID", "PURCHASE_ORDER_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_SD.SOURCE_ID.RTK", "REFERENCE_SOURCE_ID_RTK"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_SD.SOURCE_ID.ROid", "REFERENCE_SOURCE_ID_ROid"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.TAX_ID", "TAX_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.TAX_RATE", "TAX_RATE"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "AMOUNT"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "AMOUNT_UNINCLUDE_TAX_OC"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "TAX_OC"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "AMOUNT_UNINCLUDE_TAX_BC"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "TAX_BC"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "ACCEPTED_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "ACCEPTED_INVENTORY_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "RETURN_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "SCRAP_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "SP_RECEIPT_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "QUALIFIED_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "UNQUALIFIED_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "IN_DESTROYED_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "INSPECTION_STATUS"),
                                                        OOQL.CreateConstants(0, GeneralDBType.Boolean, "OVERDUE_INDICATOR"),
                                                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID", "WAREHOUSE_ID"),
                                                        Formulas.IsNull(
                                                                OOQL.CreateProperty("BIN.BIN_ID"),
                                                                OOQL.CreateConstants(idDefaultValue), "BIN_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "ITEM_LOT_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "RETURNED_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "RECEIPTED_BUSINESS_QTY"),
                                                        OOQL.CreateConstants("0", GeneralDBType.String, "RECEIPT_CLOSE"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "SCRAPED_BUSINESS_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "PIECES"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.MANUFACTURER", "MANUFACTURER"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_CERTIFICATION_D_ID", "ITEM_CERTIFICATION_D_ID"),
                                                        OOQL.CreateConstants(0, GeneralDBType.Boolean, "PAYMENT_PENDED"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_TYPE", "PURCHASE_TYPE"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.OPERATION_ID", "OPERATION_ID"),
                                                        Formulas.Case(null,
                                                                OOQL.CreateConstants("1"),
                                                                OOQL.CreateCaseArray(
                                                                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT") == OOQL.CreateConstants(0, GeneralDBType.Boolean)),
                                                                                OOQL.CreateConstants("0"))), "SN_COLLECTED_STATUS"),
                                                        Formulas.Case(null,
                                                                Formulas.IsNull(
                                                                        OOQL.CreateProperty("MO_ROUTING.MO_ID"),
                                                                        OOQL.CreateConstants(idDefaultValue)),
                                                                OOQL.CreateCaseArray(
                                                                        OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_TYPE") == OOQL.CreateConstants("2")),
                                                                                OOQL.CreateProperty("PURCHASE_ORDER_SD.SOURCE_ID.ROid"))), "MO_ID"),
                                                        OOQL.CreateConstants("N", "ApproveStatus"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "JUDGED_QTY"),
                                                        Formulas.Cast(OOQL.CreateConstants(decimalDefaultValue), GeneralDBType.Decimal, "INSPECTED_QTY"),
                                    //20170619 add by zhangcn for P001-170606002 ===beigin===
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_SD.BUDGET_ADMIN_UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BUDGET_ADMIN_UNIT_ID"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_SD.BUDGET_GROUP_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BUDGET_GROUP_ID"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_SD.BUDGET_ITEM_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BUDGET_ITEM_ID"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_SD.BUDGET_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "PRE_BUDGET_ID"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_SD.BUDGET_D_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "PRE_BUDGET_D_ID"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_D.SOURCE_ORDER.ROid"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_ORDER_ROid"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_D.SOURCE_ORDER.RTK"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_ORDER_RTK"),
                                                        Formulas.Cast(OOQL.CreateConstants(Maths.GuidDefaultValue()), GeneralDBType.Guid, "BUDGET_ID"), //需要计算
                                                        Formulas.Cast(OOQL.CreateConstants(Maths.GuidDefaultValue()), GeneralDBType.Guid, "BUDGET_D_ID"), //需要计算
                                                        OOQL.CreateConstants(Maths.GuidDefaultValue(), "INNER_ORDER_DOC_SD_ID"),
                                                        OOQL.CreateConstants(Maths.GuidDefaultValue(), "SYNERGY_SOURCE_ID_ROid"),
                                                        OOQL.CreateConstants(string.Empty, "SYNERGY_SOURCE_ID_RTK")
                                    //20170619 add by zhangcn for P001-170606002 ===end===
                                                        )
                                                    .From("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                                                    .InnerJoin(_purchaseArrivalD.Name, "A")
                                                    .On((OOQL.CreateProperty("A.PURCHASE_ORDER_SD_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID")))
                                                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                                                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID")))
                                                    .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                                                    .On((OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                                                    .InnerJoin("ITEM", "ITEM")
                                                    .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID")))
                                                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID"))
                                                                 & (OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_BUSINESS_ID")))
                                                    .InnerJoin("ITEM_PLANT", "ITEM_PLANT")
                                                    .On((OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid"))
                                                                 & (OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")))
                                                    .LeftJoin("MO_ROUTING.MO_ROUTING_D", "MO_ROUTING_D")
                                                    .On((OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.SOURCE_ID.ROid")))
                                                    .LeftJoin("MO_ROUTING", "MO_ROUTING")
                                                    .On((OOQL.CreateProperty("MO_ROUTING.MO_ROUTING_ID") == OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_ID")))
                                                    .InnerJoin("DOC", "DOC")
                                                    .On((OOQL.CreateProperty("DOC.DOC_ID") == OOQL.CreateProperty("A.DOC_ID")))
                                                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                                                    .On((OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                                                                 & (OOQL.CreateProperty("BIN.MAIN") == OOQL.CreateConstants(0, GeneralDBType.Boolean)))
                                                     .Where(OOQL.AuthFilter("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD"))
                                                     .OrderBy(OOQL.CreateOrderByItem(OOQL.CreateProperty("A.PURCHASE_ARRIVAL_ID"), SortType.Asc));
            return _querySrv.Execute(node);
        }

        /// <summary>
        /// 查询预生成到货单数据
        /// </summary>
        /// <returns></returns>
        private DataTable QueryForPurchaseArrival() {
            object idDefaultValue= Maths.GuidDefaultValue();
            QueryNode node = OOQL.Select(OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO", "DELIVERTY_NO"),
                                                        OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE", "ITEM_CODE"),
                                                        OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE", "ITEM_FEATURE_CODE"),
                                                        OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO", "ORDER_NO"),
                                                        OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE", "ORDER_SE"),
                                                        OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE", "ORDER_SE_SE"),
                                                        OOQL.CreateProperty("FIL_ARRIVAL_D.UNARR_QTY", "UNARR_QTY"),
                                                        OOQL.CreateProperty("FIL_ARRIVAL_D.ACTUAL_QTY", "ACTUAL_QTY"),
                                                        OOQL.CreateArithmetic(OOQL.CreateProperty("FIL_ARRIVAL_D.PU_QTY"),
                                                                OOQL.CreateProperty("FIL_ARRIVAL_D.RECEIPT_OVER_RATE"),
                                                                ArithmeticOperators.Mulit, "OVER_QTY"),
                                                        OOQL.CreateProperty("QTY.UNARR_QTY", "SUM_UNARR_QTY"),
                                                        OOQL.CreateProperty("QTY.OVER_QTY", "SUM_OVER_QTY"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_SD.PLAN_ARRIVAL_DATE", "PLAN_ARRIVAL_DATE"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID", "PURCHASE_ORDER_SD_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER_D.TAX_ID", "D_TAX_ID"),

                                                        OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid", "Owner_Org_ROid"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID", "SUPPLIER_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.TAX_INCLUDED", "TAX_INCLUDED"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_COMPANY_ID", "INVOICE_COMPANY_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.CURRENCY_ID", "CURRENCY_ID"),
                                                        OOQL.CreateProperty("A.DOC_ID", "DOC_ID"),
                                                        Formulas.Case(null,
                                                                OOQL.CreateConstants(idDefaultValue),
                                                                OOQL.CreateCaseArray(
                                                                        OOQL.CreateCaseItem((OOQL.CreateProperty("SUPPLIER_PURCHASE.TAX_MODE") == OOQL.CreateConstants("2")),
                                                                                OOQL.CreateProperty("SUPPLIER_PURCHASE.TAX_ID"))), "TAX_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "PURCHASE_ARRIVAL_ID"),
                                                        Formulas.GetDate("DOC_DATE"),
                                                        OOQL.CreateProperty("A.CATEGORY", "CATEGORY"),
                                                        Formulas.Cast(OOQL.CreateConstants(string.Empty), GeneralDBType.String, 20, "DOC_NO"),
                                                        Formulas.GetDate("ARRIVAL_DATE"),
                                                        OOQL.CreateConstants("SUPPLY_CENTER", GeneralDBType.String, "Owner_Org_RTK"),
                                                        
                                                        OOQL.CreateConstants("PLANT", GeneralDBType.String, "RECEIVE_Owner_Org_RTK"),
                                                        OOQL.CreateProperty("PLANT.PLANT_ID", "RECEIVE_Owner_Org_ROid"),
                                                        
                                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_FULL_NAME", "SUPPLIER_FULL_NAME"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_CONTACT_ID", "SUPPLIER_CONTACT_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_CONTACT_NAME", "SUPPLIER_CONTACT_NAME"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ADDR_ID", "SUPPLIER_ADDR_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ADDR_NAME", "SUPPLIER_ADDR_NAME"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_SUPPLIER_ID", "INVOICE_SUPPLIER_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_CONTACT_ID", "INVOICE_CONTACT_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_CONTACT_NAME", "INVOICE_CONTACT_NAME"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_ADDR_ID", "INVOICE_ADDR_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_ADDR_NAME", "INVOICE_ADDR_NAME"),
                                                        
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "RECEIPT_EMPLOYEE_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ORDER_NO", "SUPPLIER_ORDER_NO"),
                                                        
                                                        OOQL.CreateProperty("PURCHASE_ORDER.EXCHANGE_RATE", "EXCHANGE_RATE"),
                                                        
                                                        OOQL.CreateProperty("PURCHASE_ORDER.TAX_INVOICE_CATEGORY_ID", "TAX_INVOICE_CATEGORY_ID"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.PAYMENT_TERM_ID", "PAYMENT_TERM_ID"),
                                                        OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID", "Owner_Dept"),
                                                        OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID", "Owner_Emp"),
                                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String, "TAX_INVOICE_NO"),
                                                        OOQL.CreateProperty("PURCHASE_ORDER.DELIVERY_TERM_ID", "DELIVERY_TERM_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "AMOUNT_UNINCLUDE_TAX_OC"),
                                                        Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "TAX_OC"),
                                                        Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "AMOUNT_UNINCLUDE_TAX_BC"),
                                                        Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "TAX_BC"),
                                                        Formulas.Case(null,
                                                                OOQL.CreateProperty("TAX_INVOICE_CATEGORY.DEDUCTIBLE_INDICATOR"),
                                                                OOQL.CreateCaseArray(
                                                                        OOQL.CreateCaseItem((OOQL.CreateProperty("PURCHASE_ORDER.TAX_INVOICE_CATEGORY_ID") == OOQL.CreateConstants(idDefaultValue)),
                                                                                OOQL.CreateConstants(1))), "DEDUCTIBLE_INDICATOR"),
                                                        OOQL.CreateConstants("1", GeneralDBType.String, "RECEIPTED_STATUS"),
                                                        OOQL.CreateConstants(0, GeneralDBType.Boolean, "SETTLEMENT_INDICATOR"),
                                                        OOQL.CreateProperty("SUPPLIER_PURCHASE.DIRECT_SETTLEMENT_INDICATOR", "DIRECT_SETTLEMENT_INDICATOR"),
                                                        Formulas.Case(null,
                                                                OOQL.CreateConstants(0),
                                                                OOQL.CreateCaseArray(
                                                                        OOQL.CreateCaseItem((OOQL.CreateProperty("TAX_REGION.TAX_REGION_CODE") == OOQL.CreateConstants("TW", GeneralDBType.String)),
                                                                                OOQL.CreateProperty("SUPPLIER_PURCHASE.DIRECT_INVOICING_INDICATOR"))), "DIRECT_INVOICING_INDICATOR"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "SYNERGY_ID"),
                                                        Formulas.Cast(OOQL.CreateConstants(idDefaultValue), GeneralDBType.Guid, "SYNERGY_D_ID"),
                                                  //20170619 add by zhangcn for P001-170606002 ===beigin===
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.ALL_SYNERGY"), OOQL.CreateConstants(false), "ALL_SYNERGY"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.GROUP_SYNERGY_ID.ROid"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "GROUP_SYNERGY_ID_ROid"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.GROUP_SYNERGY_ID.RTK"), OOQL.CreateConstants(string.Empty), "GROUP_SYNERGY_ID_RTK"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.SOURCE_SUPPLIER_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_SUPPLIER_ID"),
                                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.DOC_Sequence"), OOQL.CreateConstants(0, GeneralDBType.Int32), "DOC_Sequence"),
                                                        OOQL.CreateConstants(string.Empty, "GENERATE_NO"),
                                                        OOQL.CreateConstants(false, "GENERATE_STATUS"),
                                                        Formulas.Cast(OOQL.CreateConstants(Maths.GuidDefaultValue()), GeneralDBType.Guid, "GROUP_SYNERGY_D_ID"), //需要计算
                                                        OOQL.CreateProperty("A.DELIVERTY_NO", "DELIVERTY_NO"),
                                                        OOQL.CreateProperty("A.ITEM_CODE", "ITEM_CODE"),
                                                        OOQL.CreateProperty("A.ORDER_NO", "ORDER_NO"),
                                                        OOQL.CreateProperty("A.ITEM_FEATURE_NO", "ITEM_FEATURE_NO"),
                                                   //20170619 add by zhangcn for P001-170606002 ===end===
                                                        OOQL.CreateConstants("N", GeneralDBType.String, "ApproveStatus"))
                                                    .From("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                                                    .InnerJoin(_tableScan.Name, "A")
                                                    .On((OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateProperty("A.DELIVERTY_NO"))
                                                        & (OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE") == OOQL.CreateProperty("A.ITEM_CODE"))
                                                        & (OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO") == OOQL.CreateProperty("A.ORDER_NO"))
                                                        & (OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE") == OOQL.CreateProperty("A.ITEM_FEATURE_NO")))
                                                    .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                                                    .On((OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO")))
                                                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                                                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID"))
                                                     & (OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE")))
                                                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                                                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID"))
                                                     & (OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE")))
                                                    .LeftJoin("EMPLOYEE", "EMPLOYEE")
                                                    .On((OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("PURCHASE_ORDER.Owner_Emp")))
                                                    .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                                                    .On((OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID") == OOQL.CreateProperty("PURCHASE_ORDER.Owner_Dept")))
                                                    .LeftJoin("PLANT", "PLANT")
                                                    .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")))
                                                    .LeftJoin("TAX_INVOICE_CATEGORY", "TAX_INVOICE_CATEGORY")
                                                    .On((OOQL.CreateProperty("TAX_INVOICE_CATEGORY.TAX_INVOICE_CATEGORY_ID") == OOQL.CreateProperty("PURCHASE_ORDER.TAX_INVOICE_CATEGORY_ID")))
                                                    .LeftJoin("SUPPLIER_PURCHASE", "SUPPLIER_PURCHASE")
                                                    .On((OOQL.CreateProperty("SUPPLIER_PURCHASE.Owner_Org.ROid") == OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid"))
                                                     & (OOQL.CreateProperty("SUPPLIER_PURCHASE.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID")))
                                                    .InnerJoin("PARA_COMPANY", "PARA_COMPANY")
                                                    .On((OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid") == OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_COMPANY_ID")))
                                                    .InnerJoin("TAX_REGION", "TAX_REGION")
                                                    .On((OOQL.CreateProperty("PARA_COMPANY.TAX_REGION_ID") == OOQL.CreateProperty("TAX_REGION.TAX_REGION_ID")))
                                                    .LeftJoin(OOQL.Select(OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO", "DOC_NO"),
                                                                          OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE", "ITEM_CODE"),
                                                                          OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE", "ITEM_FEATURE_CODE"),
                                                                          Formulas.Sum(OOQL.CreateProperty("FIL_ARRIVAL_D.UNARR_QTY"), "UNARR_QTY"),
                                                                          Formulas.Sum(OOQL.CreateProperty("FIL_ARRIVAL_D.PU_QTY")
                                                                                       * OOQL.CreateProperty("FIL_ARRIVAL_D.RECEIPT_OVER_RATE")
                                                                                       / OOQL.CreateConstants(100)
                                                                                       , "OVER_QTY"))
                                                                  .From("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                                                                  .GroupBy(OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO"),
                                                                           OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE"),
                                                                           OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE")), "QTY")
                                                    .On((OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateProperty("QTY.DOC_NO"))
                                                        & (OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE") == OOQL.CreateProperty("QTY.ITEM_CODE"))
                                                        & (OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE") == OOQL.CreateProperty("QTY.ITEM_FEATURE_CODE")))
                                                    .Where(OOQL.AuthFilter("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D"))
                                                    .OrderBy(OOQL.CreateOrderByItem(OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("PURCHASE_ORDER.TAX_INCLUDED"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_COMPANY_ID"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("PURCHASE_ORDER.COMPANY_ID"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("PURCHASE_ORDER_D.TAX_ID"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("A.DOC_ID"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("PURCHASE_ORDER_SD.PLAN_ARRIVAL_DATE"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE"), SortType.Asc),
                                                                 OOQL.CreateOrderByItem(OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE"), SortType.Desc)
                                                                 //,OOQL.CreateOrderByItem(OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO"), SortType.Desc),
                                                                 //OOQL.CreateOrderByItem(OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE"), SortType.Desc),
                                                                 //OOQL.CreateOrderByItem(OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE"), SortType.Desc)
                                                                 );
            return _querySrv.Execute(node);
        }

        /// <summary>
        /// 查询PARA_DOC_FIL对应的单据性质和单据类型
       /// </summary>
       /// <returns></returns>
        private DependencyObjectCollection QueryParaDocFil(List<ConstantsQueryProperty> docNo, List<ConstantsQueryProperty> itemCode, List<ConstantsQueryProperty> itenFeatuer) {
            //根据条件查询满足条件的单据性质和单据类型
            QueryNode node = OOQL.Select(true,
                                                        OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID"),
                                                        OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY"),
                                                        OOQL.CreateProperty("QUERY.DOC_NO"),
                                                        OOQL.CreateProperty("QUERY.ITEM_CODE"),
                                                        OOQL.CreateProperty("QUERY.ORDER_NO", "ORDER_NO"),
                                                        OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID", "SOURCE_DOC_ID"),
                                                        OOQL.CreateProperty("QUERY.ITEM_FEATURE_CODE")
                                                        )
                                                    .From("PARA_DOC_FIL", "PARA_DOC_FIL")
                                                    .InnerJoin(
                                                        OOQL.Select(
                                                                OOQL.CreateProperty("PURCHASE_ORDER.DOC_ID", "DOC_ID"),
                                                                OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid", "Owner_Org_ROid"),
                                                                OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE","ITEM_CODE"),
                                                                OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE", "ITEM_FEATURE_CODE"),
                                                                OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO", "ORDER_NO"),
                                                                OOQL.CreateProperty("FIL_ARRIVAL.DOC_NO", "DOC_NO"))
                                                            .From("FIL_ARRIVAL", "FIL_ARRIVAL")
                                                            .InnerJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                                                            .On((OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID") == OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID")))
                                                            .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                                                            .On((OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO")))
                                                            .Where((OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO").In(docNo))
                                                                    & (OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE").In(itemCode))
                                                                    & (OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE").In(itenFeatuer))), "QUERY")
                                                    .On((OOQL.CreateProperty("QUERY.Owner_Org.ROid") == OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid"))
                                                                 & ((OOQL.CreateProperty("QUERY.DOC_ID") == OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"))
                                                                    | (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))))
                                                    .Where((OOQL.AuthFilter("PARA_DOC_FIL", "PARA_DOC_FIL"))
                                                        & (OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY")==OOQL.CreateConstants("36")))
                                                    .OrderBy(
                                                        OOQL.CreateOrderByItem(
                                                                OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"), SortType.Desc));
            return GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 传入参数存入DataTable中，用于BulKCopy批量插入临时表
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDtForTableScanBulk() {
            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("DELIVERTY_NO", typeof(string)));
            dt.Columns.Add(new DataColumn("ITEM_CODE", typeof(string)));
            dt.Columns.Add(new DataColumn("ITEM_FEATURE_NO", typeof(string)));
            dt.Columns.Add(new DataColumn("DOC_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("CATEGORY", typeof(string)));
            dt.Columns.Add(new DataColumn("ORDER_NO", typeof(string)));
            return dt;
        }

        /// <summary>
        /// 预生成到货单单身相关数据，用于BulKCopy批量插入临时表
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDtForPurchaseArrivalDBulk() {
            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("BUS_QTY", typeof(decimal)));
            dt.Columns.Add(new DataColumn("PURCHASE_ORDER_SD_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("PURCHASE_ARRIVAL_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("DOC_ID", typeof(Guid)));
            return dt;
        }

        /// <summary>
        /// 参数临时表
        /// </summary>
        private void CreateTableScanTempTable() {
            IBusinessTypeService businessTypeSrv = GetServiceForThisTypeKey<IBusinessTypeService>();
            string tempName = "Table_Scan"+ "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { null });
            #region 字段
            //单号
            defaultType.RegisterSimpleProperty("DELIVERTY_NO", businessTypeSrv.SimpleDocNoType,
                string.Empty, false, businessTypeSrv.SimpleDocNo);
            //品号
            defaultType.RegisterSimpleProperty("ITEM_CODE", businessTypeSrv.SimpleItemCodeType,
                string.Empty, false, businessTypeSrv.SimpleItemCode );
            //特征码
            defaultType.RegisterSimpleProperty("ITEM_FEATURE_NO", businessTypeSrv.SimpleItemFeatureType,
                string.Empty, false, businessTypeSrv.SimpleItemFeature);
            //单据类型
            defaultType.RegisterSimpleProperty("DOC_ID", businessTypeSrv.SimplePrimaryKeyType,
                Maths.GuidDefaultValue(), false, businessTypeSrv.SimplePrimaryKey);
            //单据性质
            defaultType.RegisterSimpleProperty("CATEGORY", typeof(string),
                string.Empty, false, new Attribute[] { new SimplePropertyAttribute(GeneralDBType.String) });
            //采购订单主键
            defaultType.RegisterSimpleProperty("ORDER_NO", businessTypeSrv.SimpleDocNoType,
                string.Empty, false, businessTypeSrv.SimpleDocNo);
            #endregion
            _tableScan = defaultType;
            _querySrv.CreateTempTable(_tableScan);
        }

        /// <summary>
        /// 参数临时表
        /// </summary>
        private void CreatePurchaseArrivalDTempTable() {
            IBusinessTypeService businessTypeSrv = GetServiceForThisTypeKey<IBusinessTypeService>();
            string tempName = "PurchaseArrivalD"+ "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { null });
            #region 字段
            //到货单主键
            defaultType.RegisterSimpleProperty("PURCHASE_ARRIVAL_ID", businessTypeSrv.SimplePrimaryKeyType,
                Maths.GuidDefaultValue(), false,  businessTypeSrv.SimplePrimaryKey );
            //采购订单单身主键
            defaultType.RegisterSimpleProperty("PURCHASE_ORDER_SD_ID", businessTypeSrv.SimplePrimaryKeyType,
                Maths.GuidDefaultValue(), false, businessTypeSrv.SimplePrimaryKey);
            //单据类型
            defaultType.RegisterSimpleProperty("DOC_ID", businessTypeSrv.SimplePrimaryKeyType,
                Maths.GuidDefaultValue(), false, businessTypeSrv.SimplePrimaryKey);
            //数量
            defaultType.RegisterSimpleProperty("BUS_QTY", businessTypeSrv.SimpleQuantityType,
                0M, false, new Attribute[] { businessTypeSrv.SimpleQuantity });
            #endregion
            _purchaseArrivalD = defaultType;
            _querySrv.CreateTempTable(_purchaseArrivalD);
        }

        /// <summary>
        /// 临时表插入数据
        /// </summary>
        /// <param name="data"></param>
        private void CreateTableScan(DataTable data) {
            List<BulkCopyColumnMapping> dtScanMapping = GetBulkCopyColumnMapping(data.Columns);
            _querySrv.BulkCopy(data, _tableScan.Name, dtScanMapping.ToArray());
        }

        /// <summary>
        ///     通过DataTable列名转换为ColumnMappings
        /// </summary>
        /// <param name="columns">表的列的集合</param>
        /// <returns>Mapping集合</returns>
        private List<BulkCopyColumnMapping> GetBulkCopyColumnMapping(DataColumnCollection columns) {
            List<BulkCopyColumnMapping> mapping = new List<BulkCopyColumnMapping>();
            foreach (DataColumn column in columns) {
                var targetName = column.ColumnName;//列名
                //列名中的下划线大于0，且以[_RTK]或[_ROid]结尾的列名视为多来源字段
                if ((targetName.IndexOf("_", StringComparison.Ordinal) > 0)
                    && (targetName.EndsWith("_RTK", StringComparison.CurrentCultureIgnoreCase)
                        || targetName.EndsWith("_ROid", StringComparison.CurrentCultureIgnoreCase))) {
                    //列名长度
                    var nameLength = targetName.Length;
                    //最后一个下划线后一位位置
                    var endPos = targetName.LastIndexOf("_", StringComparison.Ordinal) + 1;
                    //拼接目标字段名
                    targetName = targetName.Substring(0, endPos - 1) + "." +
                                 targetName.Substring(endPos, nameLength - endPos);
                }
                BulkCopyColumnMapping mappingItem = new BulkCopyColumnMapping(column.ColumnName, targetName);
                mapping.Add(mappingItem);
            }
            return mapping;
        }

        //20170619 add by zhangcn for P001-170606002 ===beigin===
        private DependencyObjectCollection QuerySupplySyneryFiD(object deliveryNo, object itemCode,object orderNo,object itemFeatureCode) {
            QueryNode queryNode =
                OOQL.Select(1,
                            OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SUPPLY_SYNERGY_FI_D_ID", "SUPPLY_SYNERGY_FI_D_ID"),
                            OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SequenceNumber", "SequenceNumber"))
                    .From("SUPPLY_SYNERGY.SUPPLY_SYNERGY_FI_D", "SUPPLY_SYNERGY_FI_D")
                    .InnerJoin("SUPPLY_SYNERGY")
                    .On(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID") == OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SUPPLY_SYNERGY_ID"))
                    .InnerJoin("PURCHASE_ORDER")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER.GROUP_SYNERGY_ID.ROid") == OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID"))
                    .InnerJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                    .On(OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO") == OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"))
                    .Where(OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateConstants(deliveryNo) &
                           OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE") == OOQL.CreateConstants(itemCode) &
                           OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO") == OOQL.CreateConstants(orderNo) &
                           OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE") == OOQL.CreateConstants(itemFeatureCode)
                    )
                    .OrderBy(new OrderByItem(OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SequenceNumber"), SortType.Asc));

            return this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        //20170619 add by zhangcn for P001-170606002 ===end===
    }
}
