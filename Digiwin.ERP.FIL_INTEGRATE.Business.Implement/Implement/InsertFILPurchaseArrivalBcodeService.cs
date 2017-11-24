//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-3-27</createDate>
//<IssueNo>P001-170316001</IssueNo>
//<description>依送货单条码生成到货单服务实现</description>
//----------------------------------------------------------------
//20170330 modi by wangrm for P001-170328001
//20170508 modi by liwei1 for P001-161209002
//20170619 modi by zhangcn for P001-170606002
//20170630 modi by zhangcn for B001-170629006 更新送货单单身的状态修改
//20170711 add by zhangcn for B002-170710028 生成到货单IInsertPurchaseArrivalService，金额字段与订单有点尾差
//20170919 modi by liwei1 for B001-170918011 商品类型为赠备品时，计价数量为0

using System;
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
    [ServiceClass(typeof(IInsertFILPurchaseArrivalBcodeService))]
    [Description("依送货单条码生成到货单服务")]
    class InsertFILPurchaseArrivalBcodeService : ServiceComponent, IInsertFILPurchaseArrivalBcodeService {
        IDataEntityType _Table_scan;
        IDataEntityType _Table_scan_detail;
        IQueryService _qurService;
        private IBudgetService _budgetSrv; //预算公共服务 //20170619 add by zhangcn for P001-170606002
        bool _isUpdateBCLine = false;//20170411 add by wangrm for P001-170316001
        
        #region IInsertFILPurchaseArrivalBcodeService 成员

        public DependencyObjectCollection InsertFILPurchaseArrivalBcode(string employee_no, string scan_type, DateTime report_datetime, string picking_department_no, 
            string recommended_operations, string recommended_function, string scan_doc_no, Digiwin.Common.Torridity.DependencyObjectCollection scanColl) {
            IInfoEncodeContainer infoContainer = GetService<IInfoEncodeContainer>();
            _budgetSrv = GetServiceForThisTypeKey<IBudgetService>(); //20170619 add by zhangcn for P001-170606002
            if (string.IsNullOrEmpty(recommended_operations)) {
                throw new BusinessRuleException(string.Format(infoContainer.GetMessage("A111201"), "recommended_operations"));
            }
            if (string.IsNullOrEmpty(recommended_function)) {
                throw new BusinessRuleException(string.Format(infoContainer.GetMessage("A111201"), "recommended_function"));
            }
            if (string.IsNullOrEmpty(recommended_operations)) {
                throw new BusinessRuleException(string.Format(infoContainer.GetMessage("A111201"), "recommended_operations"));
            }
            string category = string.Empty;
            if (recommended_operations == "1-1") {
                category = "36";
            } else if (recommended_operations == "3-1") {
                category = "37";
            }
            string docNo = ((scanColl[0]["scan_detail"] as DependencyObjectCollection).FirstOrDefault() as DependencyObject)["doc_no"].ToStringExtension();
            //取的传入参数送货单号，会且只会存在一笔记录，取第一笔即可
            string deliveryNo = scanColl[0]["info_lot_no"].ToStringExtension();//20170508 add by liwei1 for P001-161209002

            object docId = QueryDocId(category, scanColl);
            if (Maths.IsEmpty(docId)) {
                throw new BusinessRuleException(infoContainer.GetMessage("A111275"));
            }
            UtilsClass utilsClass = new UtilsClass();
            DependencyObjectCollection resultColl = utilsClass.CreateReturnCollection();
            using (IConnectionService connectionService = this.GetService<IConnectionService>()) {
                _qurService = this.GetService<IQueryService>();
                CreateTempTable();
                InsertToScan(employee_no, picking_department_no, scanColl);
                using (ITransactionService transActionService = this.GetService<ITransactionService>()) {
                    InsertBC_LINE(recommended_operations);//20170406 modi by wangrm for P001-170316001 增加入参
                    InsertPA(report_datetime, category, resultColl, docId,docNo
                        , deliveryNo//20170508 add by liwei1 for P001-161209002
                        );
                    
                    transActionService.Complete();
                }
            }
            return resultColl;
            
        }

        #endregion

        /// <summary>
        /// 查询单据性质ID
        /// </summary>
        /// <param name="category">单据性质</param>
        /// <param name="scanColl">单身数据集</param>
        /// <returns>单据性质ID</returns>
        private object QueryDocId(string category, DependencyObjectCollection scanColl) {
            QueryNode node = null;
            if (scanColl.Count > 0) {
                //string infoLotNo = scanColl[0]["info_lot_no"].ToStringExtension(); //信息批号
                string docNo = ((scanColl[0]["scan_detail"] as DependencyObjectCollection).FirstOrDefault() as DependencyObject)["doc_no"].ToStringExtension();
                string mainOrganization = scanColl[0]["main_organization"].ToStringExtension(); //工厂
                //根据条件查询满足条件的DOC_ID
                node = OOQL.Select(1, OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID"))
                    .From("PARA_DOC_FIL", "PARA_DOC_FIL")
                    .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                    .On((OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID") == OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid")))
                    .Where((OOQL.AuthFilter("PARA_DOC_FIL", "PARA_DOC_FIL"))
                           & ((OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE") == OOQL.CreateConstants(mainOrganization))
                           & (OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants(category))
                           & ((OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                            | (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.Select(1, OOQL.CreateProperty("DOC_ID"))
                                .From("PURCHASE_ORDER")
                                .Where((OOQL.CreateProperty("DOC_NO") == OOQL.CreateConstants(docNo)))))))
                    .OrderBy(
                        OOQL.CreateOrderByItem(
                            OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"), SortType.Desc));
            }
            return GetService<IQueryService>().ExecuteScalar(node);
        }

        #region 临时表准备
        private void CreateTempTable() {
            IBusinessTypeService businessTypeSrv = GetServiceForThisTypeKey<IBusinessTypeService>();
            #region 单头临时表
            string tempName = "Table_scan" + "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { null });
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);

            #region 字段
            //主键
            defaultType.RegisterSimpleProperty("ID", businessTypeSrv.SimplePrimaryKeyType,
                                               Maths.GuidDefaultValue(), false, new Attribute[] { businessTypeSrv.SimplePrimaryKey });

            //人员
            defaultType.RegisterSimpleProperty("employee_no", businessTypeSrv.GetBusinessType("BusinessCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("BusinessCode") });
            tempAttr.Size = 10;
            //部门
            defaultType.RegisterSimpleProperty("picking_department_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            //工厂编号
            defaultType.RegisterSimpleProperty("site_no", businessTypeSrv.GetBusinessType("Factory"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("Factory") });
            tempAttr.Size = 30;
            //信息批号
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            //主营组织
            defaultType.RegisterSimpleProperty("main_organization", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            #endregion

            _Table_scan = defaultType;
            _qurService.CreateTempTable(_Table_scan);
            #endregion

            #region 单身临时表
            tempName = "Table_scan_detail" + "_" + DateTime.Now.ToString("HHmmssfff");
            defaultType = new DependencyObjectType(tempName, new Attribute[] { null });
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            #region 字段
            //主键
            defaultType.RegisterSimpleProperty("ID", businessTypeSrv.SimplePrimaryKeyType,
                                              Maths.GuidDefaultValue(), false, new Attribute[] { businessTypeSrv.SimplePrimaryKey });

            tempAttr.Size = 30;
            //信息批号
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            //序号
            defaultType.RegisterSimpleProperty("SequenceNumber", typeof(int),
                                                0, false, new Attribute[] { tempAttr });
            //品号
            defaultType.RegisterSimpleProperty("item_no", businessTypeSrv.GetBusinessType("ItemCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("ItemCode") });
            //特征码
            defaultType.RegisterSimpleProperty("item_feature_no", businessTypeSrv.GetBusinessType("ItemFeature"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("ItemFeature") });
            //单位
            defaultType.RegisterSimpleProperty("picking_unit_no", businessTypeSrv.GetBusinessType("UnitCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("UnitCode") });
            //单号
            defaultType.RegisterSimpleProperty("doc_no", businessTypeSrv.GetBusinessType("DocNo"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("DocNo") });
            //来源序号
            defaultType.RegisterSimpleProperty("seq", typeof(int),
                                              0, false, new Attribute[] { tempAttr });
            //来源项序
            defaultType.RegisterSimpleProperty("line_seq", typeof(int),
                                              0, false, new Attribute[] { tempAttr });
            //仓库
            defaultType.RegisterSimpleProperty("warehouse_no", businessTypeSrv.GetBusinessType("WarehouseCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("WarehouseCode") });
            //库位
            defaultType.RegisterSimpleProperty("storage_spaces_no", businessTypeSrv.GetBusinessType("Bin"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("Bin") });
            //批号
            defaultType.RegisterSimpleProperty("lot_no", businessTypeSrv.GetBusinessType("LotCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("LotCode") });
            //拣货数量
            defaultType.RegisterSimpleProperty("picking_qty", businessTypeSrv.GetBusinessType("Quantity"),
                                               0M, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("Quantity") });
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            tempAttr.Size = 1000;
            //条码编号
            defaultType.RegisterSimpleProperty("barcode_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            #endregion
            _Table_scan_detail = defaultType;
            _qurService.CreateTempTable(_Table_scan_detail);
            #endregion
        }

        private void InsertToScan(string employee_no, string picking_department_no, DependencyObjectCollection scanColl) {
            DataTable dtScan = CreateDtForScanBulk();
            DataTable dtScanDetail = CreateDtForScan_DetailBulk();
            IPrimaryKeyService prikeyService = this.GetService<IPrimaryKeyService>("PURCHASE_ARRIVAL");
            foreach (DependencyObject scanObj in scanColl) {
                DataRow drNew = dtScan.NewRow();
                #region 单头新增
                drNew["ID"] = prikeyService.CreateId();
                drNew["employee_no"] = employee_no;
                drNew["picking_department_no"] = picking_department_no;
                drNew["site_no"] = scanObj["site_no"];
                drNew["info_lot_no"] = scanObj["info_lot_no"];
                drNew["main_organization"] = scanObj["main_organization"];
                dtScan.Rows.Add(drNew);
                #endregion
                DependencyObjectCollection detailColl = scanObj["scan_detail"] as DependencyObjectCollection;
                int seqNum = 0;
                List<IGrouping<string, DependencyObject>> groupDColl = detailColl.GroupBy(c => string.Concat(c["doc_no"], c["seq"], c["line_seq"], c["item_no"], c["item_feature_no"], c["picking_unit_no"], c["warehouse_no"], c["storage_spaces_no"], c["lot_no"])).ToList();//源单+来源序号+来源相序+品号+特征码+仓库+库位+批号
                foreach (IGrouping<string, DependencyObject> groupD in groupDColl) {
                    seqNum++;
                    object id = prikeyService.CreateId();
                    foreach (DependencyObject detailObj in groupD) {
                        #region 单身新增
                        DataRow drNewDetail = dtScanDetail.NewRow();
                        drNewDetail["ID"] = id;
                        drNewDetail["SequenceNumber"] = seqNum;
                        drNewDetail["info_lot_no"] = detailObj["info_lot_no"];
                        drNewDetail["item_no"] = detailObj["item_no"];
                        drNewDetail["item_feature_no"] = detailObj["item_feature_no"];
                        drNewDetail["picking_unit_no"] = detailObj["picking_unit_no"];
                        drNewDetail["doc_no"] = detailObj["doc_no"];
                        drNewDetail["seq"] = detailObj["seq"];
                        drNewDetail["line_seq"] = detailObj["line_seq"];
                        drNewDetail["warehouse_no"] = detailObj["warehouse_no"];
                        drNewDetail["storage_spaces_no"] = detailObj["storage_spaces_no"];
                        drNewDetail["lot_no"] = detailObj["lot_no"];
                        drNewDetail["picking_qty"] = detailObj["picking_qty"];
                        drNewDetail["barcode_no"] = detailObj["barcode_no"];
                        dtScanDetail.Rows.Add(drNewDetail);
                        #endregion
                    }
                }
            }
            List<BulkCopyColumnMapping> dtScanMapping = GetBulkCopyColumnMapping(dtScan.Columns);
            _qurService.BulkCopy(dtScan, _Table_scan.Name, dtScanMapping.ToArray());
            dtScanMapping = GetBulkCopyColumnMapping(dtScanDetail.Columns);
            _qurService.BulkCopy(dtScanDetail, _Table_scan_detail.Name, dtScanMapping.ToArray());
        }

        /// <summary>
        /// bulkcopy需要datatable传入
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDtForScanBulk() {
            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("ID", typeof(object)));
            dt.Columns.Add(new DataColumn("employee_no", typeof(string)));
            dt.Columns.Add(new DataColumn("picking_department_no", typeof(string)));
            dt.Columns.Add(new DataColumn("site_no", typeof(string)));
            dt.Columns.Add(new DataColumn("info_lot_no", typeof(string)));
            dt.Columns.Add(new DataColumn("main_organization", typeof(string)));
            return dt;
        }

        /// <summary>
        /// bulkcopy需要datatable传入
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDtForScan_DetailBulk() {
            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("ID", typeof(object)));
            dt.Columns.Add(new DataColumn("info_lot_no", typeof(string)));
            dt.Columns.Add(new DataColumn("SequenceNumber", typeof(int)));
            dt.Columns.Add(new DataColumn("item_no", typeof(string)));
            dt.Columns.Add(new DataColumn("item_feature_no", typeof(string)));
            dt.Columns.Add(new DataColumn("picking_unit_no", typeof(string)));
            dt.Columns.Add(new DataColumn("doc_no", typeof(string)));
            dt.Columns.Add(new DataColumn("seq", typeof(int)));
            dt.Columns.Add(new DataColumn("line_seq", typeof(int)));
            dt.Columns.Add(new DataColumn("warehouse_no", typeof(string)));
            dt.Columns.Add(new DataColumn("storage_spaces_no", typeof(string)));
            dt.Columns.Add(new DataColumn("lot_no", typeof(string)));
            dt.Columns.Add(new DataColumn("picking_qty", typeof(decimal)));
            dt.Columns.Add(new DataColumn("barcode_no", typeof(string)));
            return dt;
        }
        #endregion

        private void InsertPA(DateTime report_datetime, string category, DependencyObjectCollection resultColl, object docId, string docNo
            , string deliveryNo//20170508 add by liwei1 for P001-161209002
            ) {
            DataTable dt = QueryForPA(report_datetime, category, docId, docNo);
            ValidateParaFilDoc(dt);  
            DataTable dt_d = QueryForPA_D(category, report_datetime);
            #region 计算列 20170619 add by zhangcn for P001-170606002
            foreach (DataRow dr_d in dt_d.Rows) {
                if (Maths.IsNotEmpty(dr_d["BUDGET_ADMIN_UNIT_ID"])){
                    object[] budgetArr =_budgetSrv.GetPerformanceBudget(report_datetime, dr_d["BUDGET_GROUP_ID"], dr_d["BUDGET_ITEM_ID"],
                                                                        dr_d["BUDGET_ADMIN_UNIT_ID"], dr_d["PRE_BUDGET_ID"], dr_d["PRE_BUDGET_D_ID"]);

                    dr_d["BUDGET_ID"] = budgetArr[0];
                    dr_d["BUDGET_D_ID"] = budgetArr[1];
                }
            }
            #endregion

            if (dt.Rows.Count > 0) {
                List<QueryProperty> purchaseArrivalIds = new List<QueryProperty>();//存储到货单ID //20170405 add by wangrm for P001-170328001
                IQueryService qrySrv = GetService<IQueryService>();//20170405 add by wangrm for P001-170328001
                ICreateService createSrv = GetService<ICreateService>("PURCHASE_ARRIVAL");
                DependencyObject entity = createSrv.Create() as DependencyObject;
                ISaveService saveService = this.GetService<ISaveService>("PURCHASE_ARRIVAL");
                List<IGrouping<object, DataRow>> groupDt = dt_d.AsEnumerable().GroupBy(a => (a.Field<object>("PURCHASE_ARRIVAL_ID"))).ToList();
                IEFNETStatusStatusService efnetSrv = this.GetService<IEFNETStatusStatusService>();
                IDocumentNumberGenerateService docNumberService = this.GetService<IDocumentNumberGenerateService>("PURCHASE_ARRIVAL");
                ITaxesService taxService = this.GetServiceForThisTypeKey<ITaxesService>();
                ICurrencyPrecisionService currencyPrecisionSrv = GetServiceForThisTypeKey<ICurrencyPrecisionService>();//20170711 add by zhangcn for B002-170710028
                IItemQtyConversionService itemQtyConversionService = this.GetServiceForThisTypeKey<IItemQtyConversionService>();
                List<QueryProperty> purchaseArivaldList = new List<QueryProperty>();//20170508 add by liwei1 for P001-161209002
                foreach (DataRow dr in dt.Rows) {
                    #region 计算列 20170619 add by zhangcn for P001-170606002
                    DependencyObjectCollection collSupplySyneryFiD = QuerySupplySyneryFiD(docNo);
                    if (collSupplySyneryFiD.Count > 0) {
                        dr["GROUP_SYNERGY_D_ID"] = collSupplySyneryFiD[0]["SUPPLY_SYNERGY_FI_D_ID"];
                    }
                    #endregion
                    DependencyObject newEntity = new DependencyObject(entity.DependencyObjectType);
                    purchaseArrivalIds.Add(OOQL.CreateConstants(dr["PURCHASE_ARRIVAL_ID"]));//20170405 add by wangrm for P001-170328001
                    DependencyObjectCollection newEntityDColl = newEntity["PURCHASE_ARRIVAL_D"] as DependencyObjectCollection;
                    AddToEntity(newEntity, dr, dt.Columns, false);
                    newEntity["DOC_NO"] = docNumberService.NextNumber(dr["DOC_ID"], dr["DOC_DATE"].ToDate().Date);
                    List<IGrouping<object, DataRow>> entityDColl = groupDt.Where(c => c.Key.Equals(dr["PURCHASE_ARRIVAL_ID"])).ToList();
                    decimal[] taxResult = new decimal[] { 0M, 0M, 0M, 0M };
                    foreach (IGrouping<object, DataRow> groupDColl in entityDColl) {
                        foreach (DataRow dr_d in groupDColl) {
                            DependencyObject newEntityD = new DependencyObject(newEntityDColl.ItemDependencyObjectType);
                            UpdatePA_D(dr, dr_d, taxService, itemQtyConversionService, currencyPrecisionSrv);//20170711 modi by zhangcn for B002-170710028 增加传参 currencyPrecisionSrv
                            AddToEntity(newEntityD, dr_d, dt_d.Columns, true);
                            newEntityDColl.Add(newEntityD);
                            taxResult[0] += dr_d["AMOUNT_UNINCLUDE_TAX_OC"].ToDecimal();
                            taxResult[1] += dr_d["TAX_OC"].ToDecimal();
                            taxResult[2] += dr_d["AMOUNT_UNINCLUDE_TAX_BC"].ToDecimal();
                            taxResult[3] += dr_d["TAX_BC"].ToDecimal();
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
                                              new List<object>() { dr["PURCHASE_ARRIVAL_ID"] });
                    DependencyObject resultObj = resultColl.AddNew();
                    resultObj["doc_no"] = newEntity["DOC_NO"];
                    purchaseArivaldList.Add(OOQL.CreateConstants(newEntity["PURCHASE_ARRIVAL_ID"]));//20170508 add by liwei1 for P001-161209002
                }

                //20170508 add by liwei1 for P001-161209002 ---begin---
                //更新送货单单身状态码
                UpdateFilArrivalD(deliveryNo, purchaseArivaldList);
                //送货单单身所有笔数的的状态码均为‘3.已收货’
                if (StatusIsOk(deliveryNo)) {
                    UpdateFilArrival(deliveryNo);
                }
                //20170508 add by liwei1 for P001-161209002 ---end---

                //20170405 add by wangrm for P001-170328001====start============
                if (_isUpdateBCLine) {
                    QueryNode node = OOQL.Select(OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.RTK", "Owner_Org_RTK"),
                                               OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.ROid", "Owner_Org_ROid"),
                                               OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_DATE", "DOC_DATE"),
                                               OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID", "SOURCE_DOC_ID"),
                                               OOQL.CreateProperty("BC_LINE.BC_LINE_ID", "BC_LINE_ID"))
                                       .From("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                                       .InnerJoin("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                                       .On(OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID"))
                                       .InnerJoin("BC_LINE", "BC_LINE")
                                       .On(OOQL.CreateProperty("BC_LINE.SOURCE_ID.RTK") == OOQL.CreateConstants("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D")
                                        & OOQL.CreateProperty("BC_LINE.SOURCE_ID.ROid") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID"))
                                        .Where(OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID").In(purchaseArrivalIds.ToArray()));
                    QueryNode updateBCLine = OOQL.Update("BC_LINE")
                                               .Set(new SetItem[]{
                                            new SetItem(OOQL.CreateProperty("Owner_Org_RTK"),OOQL.CreateProperty("SubNode.Owner_Org_RTK")),
                                            new SetItem(OOQL.CreateProperty("Owner_Org_ROid"),OOQL.CreateProperty("SubNode.Owner_Org_ROid")),
                                            new SetItem(OOQL.CreateProperty("DOC_DATE"),OOQL.CreateProperty("SubNode.DOC_DATE")),
                                            new SetItem(OOQL.CreateProperty("SOURCE_DOC_ID"),OOQL.CreateProperty("SubNode.SOURCE_DOC_ID"))
                                        })
                                               .From(node, "SubNode")
                                               .Where(OOQL.CreateProperty("BC_LINE.BC_LINE_ID") == OOQL.CreateProperty("SubNode.BC_LINE_ID"));
                    UtilsClass.ExecuteNoQuery(qrySrv, updateBCLine, false);
                }
                //20170405 add by wangrm for P001-170328001====end============
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
                    new SetItem(OOQL.CreateProperty("STATUS"), OOQL.CreateConstants("Y")),//20170630 modi by zhangcn for B001-170629006【OLD：3】
                    new SetItem(OOQL.CreateProperty("PURCHASE_ARRIVAL_ID"), OOQL.CreateProperty("A.PURCHASE_ARRIVAL_ID")),
                    new SetItem(OOQL.CreateProperty("PURCHASE_ARRIVAL_D_ID"),OOQL.CreateProperty("A.PURCHASE_ARRIVAL_D_ID"))
                })
                .From(node, "A")
                .Where((OOQL.CreateProperty("A.DOC_NO") == OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.DOC_NO"))
                       & (OOQL.CreateProperty("A.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.SequenceNumber")));
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
        private bool StatusIsOk(string deliveryNo) {
            bool isOk = true;
            QueryNode node =
                OOQL.Select(
                            Formulas.Count(
                                OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.FIL_ARRIVAL_D_ID"), "COUNT_NUM"))
                        .From("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                        .Where((OOQL.AuthFilter("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D"))
                               & ((OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.STATUS") != OOQL.CreateConstants("Y"))//20170630 modi by zhangcn for B001-170629006【OLD：3】
                               & (OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateConstants(deliveryNo))));
            //查询单身存在状态码为“3.已收货”的记录数返回FALSE，不更新单头
            if (GetService<IQueryService>().ExecuteScalar(node).ToInt32() > 0) {
                isOk = false;
            }
            return isOk;
        }

        #endregion

        private void UpdatePA_D(DataRow dr,DataRow dr_d,ITaxesService taxService,IItemQtyConversionService itemQtyConversionService,ICurrencyPrecisionService currencyPrecisionSrv){
            //20170711 modi by zhangcn for B002-170710028 增加传参 currencyPrecisionSrv
            bool inpectModeThree = dr_d["temp_INSPECT_MODE"].ToStringExtension() == "3" && dr["temp_QC_RESULT_INPUT_TYPE"].ToStringExtension() == "1";
            //20170919 add by liwei1 for B001-170918011 ===begin===
            if(dr_d["ITEM_TYPE"].ToStringExtension() != "1"){
                dr_d["PRICE_QTY"] = 0m;
            } else{
                //20170919 add by liwei1 for B001-170918011 ===end===

                if(dr_d["temp_INSPECT_MODE"].ToStringExtension() == "1" || inpectModeThree){
                    if(!(dr_d["BUSINESS_UNIT_ID"].Equals(dr_d["PRICE_UNIT_ID"]))){
                        dr_d["PRICE_QTY"] = itemQtyConversionService.TryGetConvertedQty(dr_d["ITEM_ID"],dr_d["BUSINESS_UNIT_ID"],dr_d["BUSINESS_QTY"].ToDecimal(),dr_d["PRICE_UNIT_ID"])[0];
                    } else{
                        dr_d["PRICE_QTY"] = dr_d["BUSINESS_QTY"].ToDecimal();
                    }
                    dr_d["SHOULD_SETTLE_PRICE_QTY"] = dr_d["PRICE_QTY"];
                    dr_d["DISCOUNT_AMT"] = (dr_d["PRICE"].ToDecimal() - dr_d["DISCOUNTED_PRICE"].ToDecimal()) * dr_d["PRICE_QTY"].ToDecimal();
                    dr_d["AMOUNT"] = dr_d["PRICE_QTY"].ToDecimal() * dr_d["DISCOUNTED_PRICE"].ToDecimal();
                }
            } //20170919 add by liwei1 for B001-170918011

            if(inpectModeThree){
                dr_d["QUALIFIED_BUSINESS_QTY"] = dr_d["BUSINESS_QTY"];
            }

            decimal amount = currencyPrecisionSrv.AmendAmountPrecision(dr["CURRENCY_ID"],dr_d["AMOUNT"].ToDecimal()).ToDecimal(0); //20170711 add by zhangcn for B002-170710028
            decimal[] taxResult = taxService.GetTaxes(dr["CURRENCY_ID"],dr["INVOICE_COMPANY_ID"],dr["EXCHANGE_RATE"].ToDecimal(),dr_d["TAX_ID"],dr_d["TAX_RATE"].ToDecimal(),dr["TAX_INCLUDED"].ToBoolean(),amount); //20170711 add by zhangcn for B002-170710028 OLD:dr_d["AMOUNT"].ToDecimal()
            dr_d["AMOUNT_UNINCLUDE_TAX_OC"] = taxResult[0];
            dr_d["TAX_OC"] = taxResult[1];
            dr_d["AMOUNT_UNINCLUDE_TAX_BC"] = taxResult[2];
            dr_d["TAX_BC"] = taxResult[3];
            if(dr_d["temp_INSPECT_MODE"].ToStringExtension() == "1" || inpectModeThree){
                dr_d["ACCEPTED_BUSINESS_QTY"] = dr_d["BUSINESS_QTY"];
            } else{
                dr_d["ACCEPTED_INVENTORY_QTY"] = 0m; //sql查询出来的全部默认有值，这里判断清空
            }
            if(inpectModeThree){
                dr_d["JUDGED_QTY"] = dr_d["ACCEPTED_BUSINESS_QTY"].ToDecimal() + dr_d["RETURN_BUSINESS_QTY"].ToDecimal() + dr_d["SP_RECEIPT_BUSINESS_QTY"].ToDecimal() + dr_d["SCRAP_BUSINESS_QTY"].ToDecimal();
                dr_d["INSPECTED_QTY"] = dr_d["UNQUALIFIED_BUSINESS_QTY"].ToDecimal() + dr_d["IN_DESTROYED_BUSINESS_QTY"].ToDecimal() + dr_d["QUALIFIED_BUSINESS_QTY"].ToDecimal();
            }
            switch(dr_d["temp_INSPECT_MODE"].ToStringExtension()){
                case "1":
                    dr_d["INSPECTION_STATUS"] = "1";
                    break;
                case "2":
                    dr_d["INSPECTION_STATUS"] = "2";
                    break;
                case "3":
                    if(dr["temp_QC_RESULT_INPUT_TYPE"].ToStringExtension() == "1"){
                        dr_d["INSPECTION_STATUS"] = "4";
                    } else if(dr["temp_QC_RESULT_INPUT_TYPE"].ToStringExtension() == "2"){
                        dr_d["INSPECTION_STATUS"] = "2";
                    }
                    break;
            }

        }

        private void InsertBC_LINE(string recommended_operations) {//20170406 modi by wangrm for P001-170316001 增加入参
            QueryNode node = OOQL.Select(OOQL.CreateProperty("BC_LINE_MANAGEMENT"))
               .From("PARA_FIL", "PARA_FIL");
            bool bcLineManagement = Convert.ToBoolean(_qurService.ExecuteScalar(node));
            if (bcLineManagement && recommended_operations == "3-1") {//20170406 modi by wangrm for P001-170316001 OLD： if (bcLineManagement) {
                QueryNode selectNode = QueryForBC_LINE();
                List<string> insertList = GetInsertBcLine();
                node = OOQL.Insert("BC_LINE", selectNode, insertList.ToArray());
                _qurService.ExecuteNoQueryWithManageProperties(node);
                _isUpdateBCLine = true;//20170411 add by wangrm for P001-170316001
            }
        }

        /// <summary>
        /// 校验单据类型
        /// </summary>
        /// <param name="dt"></param>
        private void ValidateParaFilDoc(DataTable dt) {
            foreach (DataRow dr in dt.Rows) {
                if (Maths.IsEmpty(dr["DOC_ID"])) {
                    throw new BusinessRuleException(this.GetService<IInfoEncodeContainer>().GetMessage("A111275"));
                }
            }
        }

        /// <summary>
        /// 单头新增查询
        /// </summary>
        /// <param name="report_datetime"></param>
        /// <param name="category"></param>
        /// <returns></returns>
        private DataTable QueryForPA(DateTime report_datetime, string category, object docId, string docNo) {
            QueryNode node = OOQL.Select(true, OOQL.CreateProperty("Table_scan.ID", "PURCHASE_ARRIVAL_ID"),
                                        Formulas.Cast(OOQL.CreateConstants(docId), GeneralDBType.Guid, "DOC_ID"),
                                        OOQL.CreateConstants(report_datetime, GeneralDBType.Date, "DOC_DATE"),
                                        OOQL.CreateConstants(category, GeneralDBType.String, "CATEGORY"),
                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String, "DOC_NO"),
                                        OOQL.CreateConstants(report_datetime, GeneralDBType.Date, "ARRIVAL_DATE"),
                                        OOQL.CreateConstants("SUPPLY_CENTER", GeneralDBType.String, "Owner_Org_RTK"),
                                        OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID", "Owner_Org_ROid"),
                                        OOQL.CreateConstants("PLANT", GeneralDBType.String, "RECEIVE_Owner_Org_RTK"),
                                        OOQL.CreateProperty("PLANT.PLANT_ID", "RECEIVE_Owner_Org_ROid"),
                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID", "SUPPLIER_ID"),
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
                                        OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_COMPANY_ID", "INVOICE_COMPANY_ID"),
                                        OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "RECEIPT_EMPLOYEE_ID"),
                                        OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ORDER_NO", "SUPPLIER_ORDER_NO"),
                                        OOQL.CreateProperty("PURCHASE_ORDER.CURRENCY_ID", "CURRENCY_ID"),
                                        OOQL.CreateProperty("PURCHASE_ORDER.EXCHANGE_RATE", "EXCHANGE_RATE"),
                                        OOQL.CreateProperty("PURCHASE_ORDER.TAX_INCLUDED", "TAX_INCLUDED"),
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
                                        Formulas.Case(null, OOQL.CreateProperty("TAX_INVOICE_CATEGORY.DEDUCTIBLE_INDICATOR"),
                                                      OOQL.CreateCaseArray(
                                                          OOQL.CreateCaseItem(
                                                              OOQL.CreateProperty("PURCHASE_ORDER.TAX_INVOICE_CATEGORY_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                                              OOQL.CreateConstants(false, GeneralDBType.Boolean))), "DEDUCTIBLE_INDICATOR"),
                                        OOQL.CreateConstants("1", GeneralDBType.String, "RECEIPTED_STATUS"),
                                        OOQL.CreateConstants(0, GeneralDBType.Boolean, "SETTLEMENT_INDICATOR"),
                                        OOQL.CreateProperty("SUPPLIER_PURCHASE.DIRECT_SETTLEMENT_INDICATOR", "DIRECT_SETTLEMENT_INDICATOR"),
                                        Formulas.Case(null, OOQL.CreateConstants(false, GeneralDBType.Boolean),
                                                      OOQL.CreateCaseArray(
                                                          OOQL.CreateCaseItem(
                                                              OOQL.CreateProperty("TAX_REGION.TAX_REGION_CODE") == OOQL.CreateConstants("TW", GeneralDBType.String),
                                                              OOQL.CreateProperty("SUPPLIER_PURCHASE.DIRECT_INVOICING_INDICATOR"))), "DIRECT_INVOICING_INDICATOR"),
                                        OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "SYNERGY_ID"),
                                        OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "SYNERGY_D_ID"),
                                        OOQL.CreateConstants("N", GeneralDBType.String, "ApproveStatus"),
                                        OOQL.CreateProperty("DOC.QC_RESULT_INPUT_TYPE", "temp_QC_RESULT_INPUT_TYPE"),
                //20170619 add by zhangcn for P001-170606002 ===beigin===
                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.ALL_SYNERGY"), OOQL.CreateConstants(false), "ALL_SYNERGY"),
                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.GROUP_SYNERGY_ID.ROid"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "GROUP_SYNERGY_ID_ROid"),
                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.GROUP_SYNERGY_ID.RTK"), OOQL.CreateConstants(string.Empty), "GROUP_SYNERGY_ID_RTK"),
                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.SOURCE_SUPPLIER_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_SUPPLIER_ID"),
                                        Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER.DOC_Sequence"), OOQL.CreateConstants(0, GeneralDBType.Int32), "DOC_Sequence"),
                                        OOQL.CreateConstants(string.Empty, "GENERATE_NO"),
                                        OOQL.CreateConstants(false, "GENERATE_STATUS"),
                                        Formulas.Cast(OOQL.CreateConstants(Maths.GuidDefaultValue()), GeneralDBType.Guid, "GROUP_SYNERGY_D_ID") //需要计算
                //20170619 add by zhangcn for P001-170606002 ===end===
                                        )
                                .From(_Table_scan.Name, "Table_scan")
                                .LeftJoin("EMPLOYEE", "EMPLOYEE")
                                .On(OOQL.CreateProperty("Table_scan.employee_no") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE"))
                                .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                                .On(OOQL.CreateProperty("Table_scan.picking_department_no") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE"))
                                .LeftJoin("PLANT", "PLANT")
                                .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                                .LeftJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                                .On(OOQL.CreateProperty("Table_scan.main_organization") == OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE"))
                                .LeftJoin("DOC", "DOC")
                                .On(OOQL.CreateProperty("DOC.DOC_ID") == OOQL.CreateConstants(docId))
                                .LeftJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                                .On(OOQL.CreateConstants(docNo) == OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"))
                                .LeftJoin("TAX_INVOICE_CATEGORY", "TAX_INVOICE_CATEGORY")
                                .On(OOQL.CreateProperty("PURCHASE_ORDER.TAX_INVOICE_CATEGORY_ID") == OOQL.CreateProperty("TAX_INVOICE_CATEGORY.TAX_INVOICE_CATEGORY_ID"))
                                .LeftJoin("SUPPLIER_PURCHASE", "SUPPLIER_PURCHASE")
                                .On(OOQL.CreateProperty("SUPPLIER_PURCHASE.Owner_Org.ROid") == OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid")
                                    & OOQL.CreateProperty("SUPPLIER_PURCHASE.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID"))
                                .InnerJoin("PARA_COMPANY")
                                .On(OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid") == OOQL.CreateProperty("PURCHASE_ORDER.INVOICE_COMPANY_ID"))
                                .InnerJoin("TAX_REGION")
                                .On(OOQL.CreateProperty("PARA_COMPANY.TAX_REGION_ID") == OOQL.CreateProperty("TAX_REGION.TAX_REGION_ID"));
            return _qurService.Execute(node);
        }
        /// <summary>
        /// 单身新增查询准备
        /// </summary>
        /// <param name="category"></param>
        /// <param name="report_datetime"></param>
        /// <returns></returns>
        private DataTable QueryForPA_D(string category, DateTime report_datetime) {
            List<QueryProperty> groupList = new List<QueryProperty>();
            groupList.Add(OOQL.CreateProperty("A.ID"));
            groupList.Add(OOQL.CreateProperty("A.info_lot_no"));
            groupList.Add(OOQL.CreateProperty("A.SequenceNumber"));
            groupList.Add(OOQL.CreateProperty("A.doc_no"));
            groupList.Add(OOQL.CreateProperty("A.seq"));
            groupList.Add(OOQL.CreateProperty("A.line_seq"));
            groupList.Add(OOQL.CreateProperty("A.item_no"));
            groupList.Add(OOQL.CreateProperty("A.item_feature_no"));
            groupList.Add(OOQL.CreateProperty("A.picking_unit_no"));
            groupList.Add(OOQL.CreateProperty("A.warehouse_no"));
            groupList.Add(OOQL.CreateProperty("A.storage_spaces_no"));
            groupList.Add(OOQL.CreateProperty("A.lot_no"));
            List<QueryProperty> selectList = new List<QueryProperty>();
            selectList.AddRange(groupList);
            selectList.Add(Formulas.Sum(OOQL.CreateProperty("A.picking_qty"), "picking_qty"));
            QueryNode groupNode = OOQL.Select(selectList.ToArray())
                .From(_Table_scan_detail.Name, "A")
                .GroupBy(groupList.ToArray());

            QueryNode node = OOQL.Select(true, OOQL.CreateProperty("Table_scan_detail.ID", "PURCHASE_ARRIVAL_D_ID"),
                                    OOQL.CreateProperty("Table_scan.ID", "PURCHASE_ARRIVAL_ID"),
                                    OOQL.CreateProperty("Table_scan_detail.SequenceNumber"),
                                    OOQL.CreateProperty("ITEM.ITEM_ID", "ITEM_ID"),
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "ITEM_DESCRIPTION"),
                                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid), "ITEM_FEATURE_ID"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_SPECIFICATION", "ITEM_SPECIFICATION"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_TYPE", "ITEM_TYPE"),
                                    OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "PACKING_MODE_ID"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "PACKING_QTY"),
                                    OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "PACKING1_UNIT_ID"),
                                    OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "PACKING2_UNIT_ID"),
                                    OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "PACKING3_UNIT_ID"),
                                    OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "PACKING4_UNIT_ID"),
                                    OOQL.CreateProperty("UNIT.UNIT_ID", "BUSINESS_UNIT_ID"),
                                    OOQL.CreateProperty("Table_scan_detail.picking_qty", "BUSINESS_QTY"),
                                    Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                            OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                            OOQL.CreateProperty ("Table_scan_detail.picking_qty"),
                                                            OOQL.CreateProperty ("ITEM.STOCK_UNIT_ID"),
                                                            OOQL.CreateConstants(0)
                                                }),
                                   Formulas.Ext("UNIT_CONVERT", "SECOND_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                            OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                            OOQL.CreateProperty ("Table_scan_detail.picking_qty"),
                                                            OOQL.CreateProperty ("ITEM.SECOND_UNIT_ID"),
                                                            OOQL.CreateConstants(0)
                                                 }),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.PRICE_UNIT_ID", "PRICE_UNIT_ID"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "PRICE_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "SHOULD_SETTLE_PRICE_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "PACKING_QTY1"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "PACKING_QTY2"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "PACKING_QTY3"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "PACKING_QTY4"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.PRICE", "PRICE"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.DISCOUNT_RATE", "DISCOUNT_RATE"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.DISCOUNTED_PRICE", "DISCOUNTED_PRICE"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "DISCOUNT_AMT"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.STANDARD_PRICE", "STANDARD_PRICE"),
                                    OOQL.CreateConstants("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", GeneralDBType.String, "SOURCE_ID_RTK"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID", "SOURCE_ID_ROid"),
                                    OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID", "PURCHASE_ORDER_ID"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_SD.SOURCE_ID.RTK", "REFERENCE_SOURCE_ID_RTK"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_SD.SOURCE_ID.ROid", "REFERENCE_SOURCE_ID_ROid"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.TAX_ID", "TAX_ID"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.TAX_RATE", "TAX_RATE"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "AMOUNT"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "AMOUNT_UNINCLUDE_TAX_OC"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "TAX_OC"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "AMOUNT_UNINCLUDE_TAX_BC"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 23, 6, "TAX_BC"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "ACCEPTED_BUSINESS_QTY"),
                                    Formulas.Ext("UNIT_CONVERT", "ACCEPTED_INVENTORY_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                            OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                            OOQL.CreateProperty ("Table_scan_detail.picking_qty"),
                                                            OOQL.CreateProperty ("ITEM.STOCK_UNIT_ID"),
                                                            OOQL.CreateConstants(0)
                                                }),//暂时直接获取有值的情况，后续不满足条件清0
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "RETURN_BUSINESS_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "SCRAP_BUSINESS_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "SP_RECEIPT_BUSINESS_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "QUALIFIED_BUSINESS_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "UNQUALIFIED_BUSINESS_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "IN_DESTROYED_BUSINESS_QTY"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String, "INSPECTION_STATUS"),
                                    OOQL.CreateConstants(false, GeneralDBType.Boolean, "OVERDUE_INDICATOR"),
                                    OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID", "WAREHOUSE_ID"),
                                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid), "BIN_ID"),
                                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid), "ITEM_LOT_ID"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "RETURNED_BUSINESS_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "RECEIPTED_BUSINESS_QTY"),
                                    Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "SCRAPED_BUSINESS_QTY"),
                                    OOQL.CreateConstants("0", GeneralDBType.String, "RECEIPT_CLOSE"),
                                     OOQL.CreateConstants(0, GeneralDBType.Int32, "PIECES"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.MANUFACTURER", "MANUFACTURER"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_CERTIFICATION_D_ID", "ITEM_CERTIFICATION_D_ID"),
                                    OOQL.CreateConstants(0, GeneralDBType.Boolean, "PAYMENT_PENDED"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_TYPE", "PURCHASE_TYPE"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.OPERATION_ID", "OPERATION_ID"),
                                    Formulas.Case(null, OOQL.CreateConstants("1", GeneralDBType.String),
                                                  OOQL.CreateCaseArray(
                                                      OOQL.CreateCaseItem(
                                                          OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT") == OOQL.CreateConstants(false, GeneralDBType.Boolean)
                                                          | OOQL.CreateProperty("Table_scan_detail.picking_qty") == OOQL.CreateConstants(0M, GeneralDBType.Decimal),
                                                          OOQL.CreateConstants("0", GeneralDBType.String))), "SN_COLLECTED_STATUS"),
                                    Formulas.Case(OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_TYPE"), OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                                  OOQL.CreateCaseArray(
                                                      OOQL.CreateCaseItem(
                                                          OOQL.CreateConstants("2", GeneralDBType.String),
                                                          OOQL.CreateProperty("PURCHASE_ORDER_SD.SOURCE_ID.ROid")),
                                                      OOQL.CreateCaseItem(
                                                          OOQL.CreateConstants("3", GeneralDBType.String),
                                                          OOQL.CreateProperty("MO_ROUTING.MO_ID"))), "MO_ID"),
                                    OOQL.CreateConstants("N", GeneralDBType.String, "ApproveStatus"),
                                     Formulas.Case(OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_TYPE"), OOQL.CreateProperty("ITEM_PLANT.INSPECT_MODE"),
                                                  OOQL.CreateCaseArray(
                                                      OOQL.CreateCaseItem(
                                                          OOQL.CreateConstants("3", GeneralDBType.String),
                                                          OOQL.CreateProperty("MO_ROUTING_D.INSPECT_MODE"))), "temp_INSPECT_MODE"),
                                      Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "JUDGED_QTY"),
                                      Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "INSPECTED_QTY"),
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
                            .From(groupNode, "Table_scan_detail")
                            .InnerJoin(_Table_scan.Name, "Table_scan")
                            .On(OOQL.CreateProperty("Table_scan_detail.info_lot_no") == OOQL.CreateProperty("Table_scan.info_lot_no"))
                            .InnerJoin("PLANT", "PLANT")
                            .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                            .InnerJoin("PARA_COMPANY")
                            .On(OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.COMPANY_ID"))
                            .InnerJoin("ITEM", "ITEM")
                            .On(OOQL.CreateProperty("Table_scan_detail.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                            .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                            .On(OOQL.CreateProperty("Table_scan_detail.item_feature_no") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE")
                                & OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                            .LeftJoin("UNIT", "UNIT")
                            .On(OOQL.CreateProperty("Table_scan_detail.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                            .LeftJoin("WAREHOUSE", "WAREHOUSE")
                            .On(OOQL.CreateProperty("Table_scan_detail.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                                & OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                            .LeftJoin("WAREHOUSE.BIN", "BIN")
                            .On(OOQL.CreateProperty("Table_scan_detail.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE")
                                & OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                            .LeftJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                            .On(OOQL.CreateProperty("Table_scan_detail.doc_no") == OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"))
                            .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                            .On((OOQL.CreateProperty("Table_scan_detail.seq") == OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber")
                                 & OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                            .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                            .On((OOQL.CreateProperty("Table_scan_detail.line_seq") == OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber")
                                 & OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID")))
                            .LeftJoin("ITEM_LOT", "ITEM_LOT")
                            .On(OOQL.CreateProperty("Table_scan_detail.lot_no") == OOQL.CreateProperty("ITEM_LOT.LOT_CODE")
                                & OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")
                                & ((OOQL.CreateProperty("Table_scan_detail.item_feature_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String)
                                    & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                                   | (OOQL.CreateProperty("Table_scan_detail.item_feature_no") != OOQL.CreateConstants(string.Empty, GeneralDBType.String)
                                & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))))
                            .LeftJoin("MO_ROUTING.MO_ROUTING_D", "MO_ROUTING_D")
                            .On(OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.SOURCE_ID.ROid"))
                            .LeftJoin("MO_ROUTING", "MO_ROUTING")
                            .On(OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_ID") == OOQL.CreateProperty("MO_ROUTING.MO_ROUTING_ID"))
                            .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                            .On(OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")
                                & OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"));
            return _qurService.Execute(node);
        }
        /// <summary>
        /// 实体赋值
        /// </summary>
        /// <param name="targetObj"></param>
        /// <param name="dr"></param>
        /// <param name="dcColl"></param>
        private void AddToEntity(DependencyObject targetObj, DataRow dr, DataColumnCollection dcColl, bool isIgnorePAId) {
            foreach (DataColumn dc in dcColl) {
                if (dc.ColumnName.StartsWith("temp"))
                    continue;//单头多查询了这类的字段做后续计算用
                string targetName = dc.ColumnName;//列名
                //列名中的下划线大于0，且以[_RTK]或[_ROid]结尾的列名视为多来源字段
                if ((targetName.IndexOf("_") > 0)
                    && (targetName.EndsWith("_RTK", StringComparison.CurrentCultureIgnoreCase)
                        || targetName.EndsWith("_ROid", StringComparison.CurrentCultureIgnoreCase))) {
                    //最后一个下划线后一位位置
                    var endPos = targetName.LastIndexOf("_") + 1;
                    //拼接目标字段名
                    string firstName = targetName.Substring(0, endPos - 1);
                    string endName = targetName.Substring(endPos, targetName.Length - endPos);
                    ((DependencyObject)targetObj[firstName])[endName] = dr[dc.ColumnName];
                } else {
                    if (!isIgnorePAId || dc.ColumnName != "PURCHASE_ARRIVAL_ID") {
                        targetObj[dc.ColumnName] = dr[dc.ColumnName];
                    }
                }
            }
        }
        /// <summary>
        /// 条码交易明细新增查询准备
        /// </summary>
        /// <returns></returns>
        private QueryNode QueryForBC_LINE() {
            List<QueryProperty> groupList = new List<QueryProperty>();
            groupList.Add(OOQL.CreateProperty("A.ID"));
            groupList.Add(OOQL.CreateProperty("A.info_lot_no"));
            groupList.Add(OOQL.CreateProperty("A.SequenceNumber"));
            groupList.Add(OOQL.CreateProperty("A.doc_no"));
            groupList.Add(OOQL.CreateProperty("A.seq"));
            groupList.Add(OOQL.CreateProperty("A.line_seq"));
            groupList.Add(OOQL.CreateProperty("A.item_no"));
            groupList.Add(OOQL.CreateProperty("A.item_feature_no"));
            groupList.Add(OOQL.CreateProperty("A.picking_unit_no"));
            groupList.Add(OOQL.CreateProperty("A.warehouse_no"));
            groupList.Add(OOQL.CreateProperty("A.storage_spaces_no"));
            groupList.Add(OOQL.CreateProperty("A.lot_no"));
            groupList.Add(OOQL.CreateProperty("A.barcode_no"));
            List<QueryProperty> selectList = new List<QueryProperty>();
            selectList.AddRange(groupList);
            selectList.Add(Formulas.Sum(OOQL.CreateProperty("A.picking_qty"), "picking_qty"));
            QueryNode groupNode = OOQL.Select(selectList.ToArray())
                .From(_Table_scan_detail.Name, "A")
                .GroupBy(groupList.ToArray());
            QueryNode node = OOQL.Select(Formulas.NewId("BC_LINE_ID"),
                                    OOQL.CreateProperty("Table_scan_detail.barcode_no", "BARCODE_NO"),
                                    OOQL.CreateConstants("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", GeneralDBType.String, "SOURCE_ID_RTK"),
                                    OOQL.CreateProperty("Table_scan_detail.ID", "SOURCE_ID_ROid"),
                                    Formulas.Ext("UNIT_CONVERT", "QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                                OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                                OOQL.CreateProperty("Table_scan_detail.picking_qty"),
                                                                OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                                OOQL.CreateConstants(0)
                                    }),
                                    OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID", "WAREHOUSE_ID"),
                                    Formulas.Case(null, OOQL.CreateProperty("BIN.BIN_ID"),
                                                  OOQL.CreateCaseArray(
                                                      OOQL.CreateCaseItem(
                                                          OOQL.CreateProperty("Table_scan_detail.storage_spaces_no") == OOQL.CreateConstants("", GeneralDBType.String),
                                                          OOQL.CreateConstants(Maths.GuidDefaultValue()))), "BIN_ID")
                                    //20170330 add by wangrm for P001-170328001=====start=======
                                    ,Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.RTK"),OOQL.CreateConstants(string.Empty), "Owner_Org_RTK")
                                    ,Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.Owner_Org.ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org_ROid")
                                    , Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_DOC_ID")
                                    , Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_DATE"),OOQL.CreateConstants(OrmDataOption.EmptyDateTime), "DOC_DATE")
                                    //20170330 add by wangrm for P001-170328001=====end=======
                                                          )
                            .From(groupNode, "Table_scan_detail")
                            .InnerJoin(_Table_scan.Name, "Table_scan")
                            .On(OOQL.CreateProperty("Table_scan_detail.info_lot_no") == OOQL.CreateProperty("Table_scan.info_lot_no"))
                            .InnerJoin("PLANT", "PLANT")
                            .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                            .InnerJoin("ITEM", "ITEM")
                            .On(OOQL.CreateProperty("Table_scan_detail.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                            .InnerJoin("UNIT", "UNIT")
                            .On(OOQL.CreateProperty("Table_scan_detail.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                            .LeftJoin("WAREHOUSE", "WAREHOUSE")
                            .On((OOQL.CreateProperty("Table_scan_detail.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                                 & OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                            .LeftJoin("WAREHOUSE.BIN", "BIN")
                            .On((OOQL.CreateProperty("Table_scan_detail.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE")
                                 & OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID")))
                              //20170330 add by wangrm for P001-170328001=====start=======
                             .LeftJoin("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                             .On(OOQL.CreateProperty("Table_scan.ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID"))
                              //20170330 add by wangrm for P001-170328001=====end=======
                            .Where(OOQL.CreateProperty("Table_scan_detail.barcode_no") != OOQL.CreateConstants(string.Empty, GeneralDBType.String));
            return node;
        }
        /// <summary>
        /// 条码校验明细新增字段准备
        /// </summary>
        /// <returns></returns>
        private List<string> GetInsertBcLine() {
            List<string> insertString = new List<string>();
            insertString.Add("BC_LINE_ID");
            insertString.Add("BARCODE_NO");
            insertString.Add("SOURCE_ID_RTK");
            insertString.Add("SOURCE_ID_ROid");
            insertString.Add("QTY");
            insertString.Add("WAREHOUSE_ID");
            insertString.Add("BIN_ID");
            //20170330 add by wangrm for P001-170328001=====start=======
            insertString.Add("Owner_Org_RTK");
            insertString.Add("Owner_Org_ROid");
            insertString.Add("SOURCE_DOC_ID");
            insertString.Add("DOC_DATE");
            //20170330 add by wangrm for P001-170328001=====end=======
            return insertString;
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
                if ((targetName.IndexOf("_") > 0)
                    && (targetName.EndsWith("_RTK", StringComparison.CurrentCultureIgnoreCase)
                        || targetName.EndsWith("_ROid", StringComparison.CurrentCultureIgnoreCase))) {
                    //列名长度
                    var nameLength = targetName.Length;
                    //最后一个下划线后一位位置
                    var endPos = targetName.LastIndexOf("_") + 1;
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
        private DependencyObjectCollection QuerySupplySyneryFiD(object docNo) {
            QueryNode queryNode =
                OOQL.Select(1,
                            OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SUPPLY_SYNERGY_FI_D_ID", "SUPPLY_SYNERGY_FI_D_ID"),
                            OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SequenceNumber", "SequenceNumber"))
                    .From("SUPPLY_SYNERGY.SUPPLY_SYNERGY_FI_D", "SUPPLY_SYNERGY_FI_D")
                    .InnerJoin("SUPPLY_SYNERGY")
                    .On(OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID") ==
                        OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SUPPLY_SYNERGY_ID"))
                    .InnerJoin("PURCHASE_ORDER")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER.GROUP_SYNERGY_ID.ROid") ==
                        OOQL.CreateProperty("SUPPLY_SYNERGY.SUPPLY_SYNERGY_ID"))
                    .Where(OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateConstants(docNo))
                    .OrderBy(new OrderByItem(OOQL.CreateProperty("SUPPLY_SYNERGY_FI_D.SequenceNumber"), SortType.Asc));

            return this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        //20170619 add by zhangcn for P001-170606002 ===end===
    }
}
