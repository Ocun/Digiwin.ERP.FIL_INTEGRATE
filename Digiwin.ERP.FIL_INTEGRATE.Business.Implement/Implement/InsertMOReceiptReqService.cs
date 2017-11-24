//---------------------------------------------------------------- 
//<Author>shenbao</Author>
//<CreateDate>2017/07/25 10:17:43</CreateDate>
//<IssueNO>P001-170717001</IssueNO>
//<Description>产生生产入库申请单服务,从InsertMOReceiptService同步过来</Description>
//---------------------------------------------------------------- 

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using Digiwin.ERP.EFNET.Business;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 产生生产入库申请单 
    /// </summary>
    [ServiceClass(typeof(IInsertMOReceiptReqService))]
    [Description("产生生产入库申请单")]
    sealed class InsertMOReceiptReqService : ServiceComponent, IInsertMOReceiptReqService {
        IDataEntityType _Table_scan;
        IDataEntityType _Table_scan_detail;
        IQueryService _qurService;

        /// <summary>
        /// 产生生产入库申请单
        /// </summary>
        /// <param name="employee_no">扫描人员</param>
        /// <param name="scan_type">扫描类型1.有箱条码 2.无箱条码</param>
        /// <param name="report_datetime">上传时间</param>
        /// <param name="picking_department_no">领料部门</param>
        /// <param name="recommended_operations">建议执行作业</param>
        /// <param name="recommended_function">A.新增  S.过帐</param>
        /// <param name="scan_doc_no">扫描单号</param>
        /// <param name="scanColl"></param>
        public DependencyObjectCollection InsertMOReceiptReq(string employee_no, string scan_type, DateTime report_datetime, string picking_department_no,
            string recommended_operations, string recommended_function, string scan_doc_no, DependencyObjectCollection scanColl) {
            IInfoEncodeContainer infoContainer = GetService<IInfoEncodeContainer>();
            if (string.IsNullOrEmpty(recommended_operations)) {
                throw new BusinessRuleException(string.Format(infoContainer.GetMessage("A111201"), "recommended_operations"));
            }
            if (string.IsNullOrEmpty(recommended_function)) {
                throw new BusinessRuleException(string.Format(infoContainer.GetMessage("A111201"), "recommended_function"));
            }
            string category = "59";
            //20170209 add by liwei1 for P001-170203001 ===begin===
            object docId = QueryDocId(category, recommended_operations, scanColl);
            if (Maths.IsEmpty(docId)) {
                throw new BusinessRuleException(infoContainer.GetMessage("A111275"));
            }
            //20170209 add by liwei1 for P001-170203001 ===end===
            UtilsClass utilsClass = new UtilsClass();
            DependencyObjectCollection resultColl = utilsClass.CreateReturnCollection();
            using (IConnectionService connectionService = this.GetService<IConnectionService>()) {
                _qurService = this.GetService<IQueryService>();
                CreateTempTable();
                InsertToScan(employee_no, picking_department_no, scanColl);
                InsertMR(report_datetime, category, recommended_operations, resultColl, docId);
            }
            return resultColl;
        }

        #region 临时表准备
        private void CreateTempTable() {
            IBusinessTypeService businessTypeSrv = GetServiceForThisTypeKey<IBusinessTypeService>();
            #region 单头临时表
            string tempName = "Table_scan" + "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { null });
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);

            #region 字段
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
            IPrimaryKeyService prikeyService = this.GetService<IPrimaryKeyService>("MO_RECEIPT_REQUISTION");
            foreach (DependencyObject scanObj in scanColl) {
                DataRow drNew = dtScan.NewRow();
                #region 单头新增
                drNew["ID"] = prikeyService.CreateId();
                drNew["employee_no"] = employee_no;
                drNew["picking_department_no"] = picking_department_no;
                drNew["site_no"] = scanObj["site_no"];
                drNew["info_lot_no"] = scanObj["info_lot_no"];
                dtScan.Rows.Add(drNew);
                #endregion
                DependencyObjectCollection detailColl = scanObj["scan_detail"] as DependencyObjectCollection;
                int seqNum = 0;
                List<IGrouping<string, DependencyObject>> groupDColl = detailColl.GroupBy(c => string.Concat(c["doc_no"], c["seq"], c["item_no"], c["item_feature_no"], c["picking_unit_no"], c["warehouse_no"], c["storage_spaces_no"], c["lot_no"])).ToList();//源单+来源序号+品号+特征码+仓库+库位+批号
                foreach (IGrouping<string, DependencyObject> groupD in groupDColl) {
                    seqNum++;
                    object id = prikeyService.CreateId();
                    foreach (DependencyObject detailObj in groupD) {
                        DataRow drNewDetail = dtScanDetail.NewRow();
                        #region 单身新增
                        drNewDetail["ID"] = id;
                        drNewDetail["SequenceNumber"] = seqNum;
                        drNewDetail["info_lot_no"] = detailObj["info_lot_no"];
                        drNewDetail["item_no"] = detailObj["item_no"];
                        drNewDetail["item_feature_no"] = detailObj["item_feature_no"];
                        drNewDetail["picking_unit_no"] = detailObj["picking_unit_no"];
                        drNewDetail["doc_no"] = detailObj["doc_no"];
                        drNewDetail["seq"] = detailObj["seq"];
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
            dt.Columns.Add(new DataColumn("warehouse_no", typeof(string)));
            dt.Columns.Add(new DataColumn("storage_spaces_no", typeof(string)));
            dt.Columns.Add(new DataColumn("lot_no", typeof(string)));
            dt.Columns.Add(new DataColumn("picking_qty", typeof(decimal)));
            dt.Columns.Add(new DataColumn("barcode_no", typeof(string)));
            return dt;
        }
        #endregion

        //20170209 add by liwei1 for P001-170203001 ===begin===
        /// <summary>
        /// 查询单据性质ID
        /// </summary>
        /// <param name="category">单据性质</param>
        /// <param name="scanColl">单身数据集</param>
        /// <returns>单据性质ID</returns>
        private object QueryDocId(string category, string recommendedOperations, DependencyObjectCollection scanColl) {
            QueryNode node = null;
            if (scanColl.Count > 0) {
                string infoLotNo = scanColl[0]["info_lot_no"].ToStringExtension(); //信息批号
                string siteNo = scanColl[0]["site_no"].ToStringExtension(); //工厂
                node = OOQL.Select(1, OOQL.CreateProperty("MO.DOC_ID"))
                                    .From("MO", "MO")
                                    .Where((OOQL.AuthFilter("MO", "MO"))
                                        & (OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(infoLotNo)));
                //根据条件查询满足条件的DOC_ID
                node = OOQL.Select(1, OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID"))
                    .From("PARA_DOC_FIL", "PARA_DOC_FIL")
                    .InnerJoin("PLANT", "PLANT")
                    .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid")))
                    .Where((OOQL.AuthFilter("PARA_DOC_FIL", "PARA_DOC_FIL"))
                           & ((OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo))
                           & (OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants(category))
                           & ((OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                            | (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == node))))
                    .OrderBy(
                        OOQL.CreateOrderByItem(
                            OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"), SortType.Desc));
            }
            return GetService<IQueryService>().ExecuteScalar(node);
        }
        //20170209 add by liwei1 for P001-170203001 ===end===

        private void InsertMR(DateTime report_datetime, string category, string recommendedOperations, DependencyObjectCollection resultColl, object docId) {//20170209 modi by liwei1 for P001-170124001 增加参数：docId
            DataTable dt = QueryForMoReceiptReq(report_datetime, category, recommendedOperations, docId);//20170209 modi by liwei1 for P001-170124001 增加参数：docId
            ValidateParaFilDoc(dt);  //20161213 add by shenbao for B001-161213006
            DataTable dt_d = QueryForMoReceipt_D(category, report_datetime, recommendedOperations);
            if (dt.Rows.Count > 0) {
                IQueryService qrySrv = GetService<IQueryService>();//20170405 add by wangrm for P001-170328001
                ICreateService createSrv = GetService<ICreateService>("MO_RECEIPT_REQUISTION");
                DependencyObject entity = createSrv.Create() as DependencyObject;
                ISaveService saveService = this.GetService<ISaveService>("MO_RECEIPT_REQUISTION");
                List<IGrouping<object, DataRow>> groupDt = dt_d.AsEnumerable().GroupBy(a => (a.Field<object>("MO_RECEIPT_REQUISTION_ID"))).ToList();
                IEFNETStatusStatusService efnetSrv = this.GetService<IEFNETStatusStatusService>();
                IDocumentNumberGenerateService docNumberService = this.GetService<IDocumentNumberGenerateService>("MO_RECEIPT_REQUISTION");
                foreach (DataRow dr in dt.Rows) {
                    DependencyObject newEntity = new DependencyObject(entity.DependencyObjectType);
                    DependencyObjectCollection newEntityDColl = newEntity["MO_RECEIPT_REQUISTION_D"] as DependencyObjectCollection;
                    AddToEntity(newEntity, dr, dt.Columns, false);
                    newEntity["DOC_NO"] = docNumberService.NextNumber(dr["DOC_ID"], dr["DOC_DATE"].ToDate().Date);
                    List<IGrouping<object, DataRow>> entityDColl = groupDt.Where(c => c.Key.Equals(dr["MO_RECEIPT_REQUISTION_ID"])).ToList();
                    foreach (IGrouping<object, DataRow> groupDColl in entityDColl) {
                        foreach (DataRow dr_d in groupDColl) {
                            DependencyObject newEntityD = new DependencyObject(newEntityDColl.ItemDependencyObjectType);
                            AddToEntity(newEntityD, dr_d, dt_d.Columns, true);
                            newEntityDColl.Add(newEntityD);
                        }
                    }
                    //20170428 add by wangyq for P001-170427001  ============begin==========
                    DependencyObject resultObj = resultColl.AddNew();
                    resultObj["doc_no"] = newEntity["DOC_NO"];
                    //20170428 add by wangyq for P001-170427001  ============end==========

                    //20170628 modi by zhangcn for P001-170327001 ===begin===
                    try {
                        SetIgnoreWarningTag(); //忽略警告
                        saveService.Save(newEntity);//希望触发保存校验
                    }
                    finally {
                        ResetIgnoreWarningTag();// 重置警告
                    }
                    //20170628 modi by zhangcn for P001-170327001 ===end===
                    
                    //7.3自动签核
                    efnetSrv.GetFormFlow("MO_RECEIPT_REQUISTION.I01", dr["DOC_ID"], dr["Owner_Org_ROid"],
                                              new List<object>() { dr["MO_RECEIPT_REQUISTION_ID"] });
                }
            }
        }

        //20161213 add by shenbao for B001-161213006
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
        private DataTable QueryForMoReceiptReq(DateTime report_datetime, string category, string recommended_operations, object docId) {//20170104 modi by wangyq for P001-161215001 添加参数recommended_operations  //20170209 modi by liwei1 for P001-170124001 添加参数：docId
            QueryNode node = OOQL.Select(true,
                                        Formulas.Cast(OOQL.CreateConstants(docId), GeneralDBType.Guid, "DOC_ID"),//20170209 add by liwei1 for P001-170124001
                                        OOQL.CreateConstants(report_datetime, GeneralDBType.Date, "DOC_DATE"),
                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String, "DOC_NO"),
                                        OOQL.CreateConstants(report_datetime, GeneralDBType.Date, "TRANSACTION_DATE"),
                                        OOQL.CreateConstants("PLANT", GeneralDBType.String, "Owner_Org_RTK"),
                                        OOQL.CreateProperty("PLANT.PLANT_ID", "Owner_Org_ROid"),
                                        OOQL.CreateProperty("MO.SOURCE_ID.ROid", "WORK_CENTER_ID"),
                                        Formulas.IsNull(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Dept"),//20170328 modi by wangyq for P001-170327001 添加null判断
                                        OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID", "Owner_Emp"),
                                        OOQL.CreateProperty("Table_scan.ID", "MO_RECEIPT_REQUISTION_ID"),
                                        OOQL.CreateConstants(false, GeneralDBType.Boolean, "CLOSE"),
                                        OOQL.CreateProperty("DOC.QC_RESULT_INPUT_TYPE", "temp_QC_RESULT_INPUT_TYPE")
                                        )
                                .From(_Table_scan.Name, "Table_scan")
                                .InnerJoin("PLANT", "PLANT")
                                .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                                .InnerJoin("EMPLOYEE", "EMPLOYEE")
                                .On(OOQL.CreateProperty("Table_scan.employee_no") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE"))
                                .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")//20170328 modi by wangyq for P001-170327001 old:InnerJoin
                                .On(OOQL.CreateProperty("Table_scan.picking_department_no") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE"))
                                .InnerJoin("DOC", "DOC")
                                .On(OOQL.CreateProperty("DOC.DOC_ID") == OOQL.CreateConstants(docId))//20170209 add by liwei1 for P001-170124001 old:OOQL.CreateProperty("DOC.DOC_ID"))
                                .InnerJoin(_Table_scan_detail.Name, "Table_scan_detail")
                                .On(OOQL.CreateProperty("Table_scan.info_lot_no") == OOQL.CreateProperty("Table_scan_detail.info_lot_no"))
                                .InnerJoin("MO", "MO")
                                .On(OOQL.CreateProperty("Table_scan_detail.SequenceNumber") == OOQL.CreateConstants(1, GeneralDBType.Int32)
                                    & OOQL.CreateProperty("Table_scan_detail.doc_no") == OOQL.CreateProperty("MO.DOC_NO"));

            return _qurService.Execute(node);
        }
        /// <summary>
        /// 单身新增查询准备
        /// </summary>
        /// <param name="category"></param>
        /// <param name="report_datetime"></param>
        /// <returns></returns>
        private DataTable QueryForMoReceipt_D(string category, DateTime report_datetime, string recommendedOperations) {
            List<QueryProperty> groupList = new List<QueryProperty>();
            groupList.Add(OOQL.CreateProperty("A.ID"));
            groupList.Add(OOQL.CreateProperty("A.info_lot_no"));
            groupList.Add(OOQL.CreateProperty("A.SequenceNumber"));
            groupList.Add(OOQL.CreateProperty("A.doc_no"));
            groupList.Add(OOQL.CreateProperty("A.seq"));
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
            QueryNode node = OOQL.Select(true, OOQL.CreateProperty("Table_scan_detail.SequenceNumber", "SequenceNumber"),
                                OOQL.CreateProperty("MO.MO_ID", "MO_ID"),
                                OOQL.CreateProperty("MO_PRODUCT.MO_PRODUCT_ID", "MO_PRODUCT_ID"),
                                OOQL.CreateProperty("MO_PRODUCT.PRODUCT_TYPE", "PRODUCT_TYPE"),
                                OOQL.CreateProperty("ITEM.ITEM_ID", "ITEM_ID"),
                                OOQL.CreateProperty("ITEM.ITEM_NAME", "ITEM_DESCRIPTION"),
                                Formulas.Case(null, OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                                              OOQL.CreateCaseArray(
                                                  OOQL.CreateCaseItem(
                                                      OOQL.CreateProperty("Table_scan_detail.item_feature_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String),
                                                      OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid))), "ITEM_FEATURE_ID"),
                                OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "ITEM_SPECIFICATION"),
                                OOQL.CreateProperty("Table_scan_detail.picking_qty", "REQUEST_QTY"),
                                Formulas.Ext("UNIT_CONVERT", "REQUEST_INVENTORY_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                            OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                            OOQL.CreateProperty ("Table_scan_detail.picking_qty"),
                                                            OOQL.CreateProperty ("ITEM.STOCK_UNIT_ID"),
                                                            OOQL.CreateConstants(0)
                                }),
                                Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "QUALIFIED_BUSINESS_QTY"),
                                Formulas.Case(null, OOQL.CreateConstants(0M), new CaseItem[]{
                                    new CaseItem(OOQL.CreateProperty("ITEM_PLANT.INSPECT_MODE")==OOQL.CreateConstants("1"),
                                        OOQL.CreateProperty("Table_scan_detail.picking_qty"))}, "ACCEPTED_QTY"),
                                Formulas.Case(null, OOQL.CreateConstants("2"), new CaseItem[]{
                                    new CaseItem(OOQL.CreateProperty("ITEM_PLANT.INSPECT_MODE")==OOQL.CreateConstants("1"),
                                        OOQL.CreateConstants("1"))}, "INSPECTION_STATUS"),
                                Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "SCRAP_QTY"),
                                Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "DESTROYED_QTY"),
                                OOQL.CreateProperty("UNIT.UNIT_ID", "BUSINESS_UNIT_ID"),
                                Formulas.Ext("UNIT_CONVERT", "SECOND_QTY", new object[]{OOQL.CreateProperty("ITEM.ITEM_ID"),
                                                            OOQL.CreateProperty("UNIT.UNIT_ID"),
                                                            OOQL.CreateProperty ("Table_scan_detail.picking_qty"),
                                                            OOQL.CreateProperty ("ITEM.SECOND_UNIT_ID"),
                                                            OOQL.CreateConstants(0)
                                }),
                                OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID", "WAREHOUSE_ID"),
                                Formulas.Case(null, OOQL.CreateProperty("BIN.BIN_ID"),
                                              OOQL.CreateCaseArray(
                                                  OOQL.CreateCaseItem(
                                                      OOQL.CreateProperty("Table_scan_detail.storage_spaces_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String),
                                                      OOQL.CreateConstants(Maths.GuidDefaultValue()))), "BIN_ID"),
                                Formulas.Case(null, Formulas.Case(null, OOQL.CreateConstants(Maths.GuidDefaultValue()), new CaseItem[]{
                                    new CaseItem(OOQL.CreateProperty("MO.ITEM_LOT_ID")!=OOQL.CreateConstants(Maths.GuidDefaultValue())
                                        &OOQL.CreateProperty("MO_PRODUCT.PRODUCT_TYPE")==OOQL.CreateConstants("1"),OOQL.CreateProperty("MO.ITEM_LOT_ID"))
                                        }),
                                              OOQL.CreateCaseArray(
                                                  OOQL.CreateCaseItem(
                                                      OOQL.CreateProperty("Table_scan_detail.lot_no") != OOQL.CreateConstants(string.Empty, GeneralDBType.String),
                                                      OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))), "ITEM_LOT_ID"),
                                OOQL.CreateProperty("MO.MO_ID", "SOURCE_MO_ID"),
                                OOQL.CreateConstants(string.Empty, GeneralDBType.String, "REMARK"),
                                OOQL.CreateConstants("N", GeneralDBType.String, "ApproveStatus"),
                                OOQL.CreateConstants(OrmDataOption.EmptyDateTime, GeneralDBType.Date, "ApproveDate"),
                                OOQL.CreateProperty("Table_scan.ID", "MO_RECEIPT_REQUISTION_ID"),
                                OOQL.CreateProperty("Table_scan_detail.ID", "MO_RECEIPT_REQUISTION_D_ID"),
                                OOQL.CreateConstants("0", GeneralDBType.String, "CLOSE")
                                                      )
                        .From(groupNode, "Table_scan_detail")
                        .InnerJoin(_Table_scan.Name, "Table_scan")
                        .On(OOQL.CreateProperty("Table_scan_detail.info_lot_no") == OOQL.CreateProperty("Table_scan.info_lot_no"))
                        .InnerJoin("PLANT", "PLANT")
                        .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                        .InnerJoin("ITEM", "ITEM")
                        .On(OOQL.CreateProperty("Table_scan_detail.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                        .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                        .On((OOQL.CreateProperty("Table_scan_detail.item_feature_no") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE")
                             & OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")))
                        .InnerJoin("UNIT", "UNIT")
                        .On(OOQL.CreateProperty("Table_scan_detail.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                        .InnerJoin("MO", "MO")
                        .On(OOQL.CreateProperty("Table_scan_detail.doc_no") == OOQL.CreateProperty("MO.DOC_NO"))
                        .InnerJoin("MO.MO_PRODUCT", "MO_PRODUCT")
                        .On((OOQL.CreateProperty("Table_scan_detail.seq") == OOQL.CreateProperty("MO_PRODUCT.SequenceNumber")
                             & OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_PRODUCT.MO_ID")))
                        .LeftJoin("WAREHOUSE", "WAREHOUSE")
                        .On((OOQL.CreateProperty("Table_scan_detail.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                             & OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                        .LeftJoin("WAREHOUSE.BIN", "BIN")
                        .On((OOQL.CreateProperty("Table_scan_detail.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE")
                             & OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID")))
                        .LeftJoin("ITEM_LOT", "ITEM_LOT")
                        .On((OOQL.CreateProperty("Table_scan_detail.lot_no") == OOQL.CreateProperty("ITEM_LOT.LOT_CODE")
                             & OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")
                             & ((OOQL.CreateProperty("Table_scan_detail.item_feature_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String)
                                 & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid))
                                | OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))))
                        .InnerJoin("ITEM_PLANT")
                        .On((OOQL.CreateProperty("MO_PRODUCT.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                             & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid")));
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
                if (dc.ColumnName.StartsWith("temp")) return;//多查询出来的字段做后续字段飞赋值判断
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
                    if (!isIgnorePAId || dc.ColumnName != "MO_RECEIPT_REQUISTION_ID") {
                        targetObj[dc.ColumnName] = dr[dc.ColumnName];
                    }
                }
            }
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

        #region 20170628 add by zhangcn for P001-170327001
        private void SetIgnoreWarningTag() {
            DeliverContext deliver = CallContext.GetData(DeliverContext.Name) as DeliverContext;
            if (deliver == null) {
                deliver = new DeliverContext();
                CallContext.SetData(DeliverContext.Name, deliver);
            }
            if (deliver.ContainsKey("IgnoreWarning")) {
                deliver["IgnoreWarning"] = true;
            }
            else {
                deliver.Add("IgnoreWarning", true);
            }
        }

        private void ResetIgnoreWarningTag() {
            DeliverContext deliver = CallContext.GetData(DeliverContext.Name) as DeliverContext;
            if (deliver != null && deliver.ContainsKey("IgnoreWarning")) {
                deliver["IgnoreWarning"] = false;
            }
        }

        #endregion
    }
}
