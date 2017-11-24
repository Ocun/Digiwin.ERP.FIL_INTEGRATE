//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/2/7 16:35:09</CreateDate>
//<IssueNO>B001-170206023</IssueNO>
//<Description>更新盘点计划服务实现</Description>
//----------------------------------------------------------------  
//20170302 modi by shenbao for P001-170302001  交易对象没有传值是默认为OTHER
//20170324 modi by wangyq for B001-170324010  去掉COUINT_PLAN_ID的关联
//20170505 modi by shenbao for P001-170505001 修正盘点问题

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Runtime.Remoting.Messaging;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    /// <summary>
    ///  
    /// </summary>
    [ServiceClass(typeof(IUpdateCountingPlanService))]
    [Description("")]
    sealed class UpdateCountingPlanService : ServiceComponent, IUpdateCountingPlanService {
        IQueryService _qurService;
        IDataEntityType _Table_scan;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="counting_type">盘点类型</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="counting_no">盘点计划编号</param>
        /// <param name="scan"></param>
        public void UpdateCountingPlan(string counting_type, string site_no, string counting_no, DependencyObjectCollection scan) {
            if (string.IsNullOrEmpty(counting_no)) {
                IInfoEncodeContainer infoContainer = this.GetService<IInfoEncodeContainer>();
                throw new BusinessRuleException(string.Format(infoContainer.GetMessage("A111201"), "counting_no"));
            }
            using (IConnectionService connectionService = this.GetService<IConnectionService>()) {
                _qurService = this.GetService<IQueryService>();
                CreateTempTable();//创建临时表
                InsertToScan(site_no, scan);//临时表储存scan入参
                QueryNode queryScan = QueryScan(counting_no);
                QueryNode querySumNode = QuerySumForInsert(counting_no, queryScan);
                DependencyObjectCollection sumColl = _qurService.ExecuteDependencyObject(querySumNode);
                List<DependencyObject> newList = new List<DependencyObject>();
                ISaveService saveService = this.GetService<ISaveService>("COUNTING");
                if (sumColl.Count > 0) {//查询存在记录新增实体COUNTING
                    ICreateService createSrv = GetService<ICreateService>("COUNTING");
                    DependencyObject entity = createSrv.Create() as DependencyObject;
                    newList = InsertCOUNTING(counting_type, counting_no, sumColl, entity.DependencyObjectType);
                }
                ICreateService createSrvParaFil = this.GetService<ICreateService>("PARA_FIL");
                QueryNode updateNode = null;
                QueryNode insertNode = null;
                if (createSrvParaFil != null) { //表示该typekey存在
                    bool bcManagement = GetBcInventoryManagement();
                    if (bcManagement) {
                        QueryNode querySumBarcodeNode = QuerySumBarcode(counting_no, queryScan);
                        updateNode = GetUpdateNode(counting_type, counting_no, querySumBarcodeNode);
                        insertNode = QueryNodeForInsert(counting_no, counting_type, querySumBarcodeNode);
                    }
                }
                using (ITransactionService transActionService = this.GetService<ITransactionService>()) {
                    if (newList.Count > 0) {
                        SetIgnoreWarningTag(true);
                        saveService.Save(newList.ToArray());
                        SetIgnoreWarningTag(false);
                    }
                    //更新条码盘点计划
                    if (updateNode != null) {//启用条码库存管理更新
                        _qurService.ExecuteNoQueryWithManageProperties(updateNode);

                    }
                    //新增条码盘点计划
                    if (insertNode != null) {//启用条码库存管理更新
                        _qurService.ExecuteNoQueryWithManageProperties(insertNode);
                    }
                    transActionService.Complete();
                }
            }
        }

        #region 临时表准备
        private void CreateTempTable() {
            IBusinessTypeService businessTypeSrv = GetServiceForThisTypeKey<IBusinessTypeService>();
            #region 单头临时表
            string tempName = "Table_scan" + "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { null });
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            #region 字段
            //营运据点
            defaultType.RegisterSimpleProperty("site_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            //条码编号
            defaultType.RegisterSimpleProperty("barcode_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            //料件编号
            defaultType.RegisterSimpleProperty("item_no", businessTypeSrv.GetBusinessType("ItemCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("ItemCode") });
            //产品特征
            defaultType.RegisterSimpleProperty("item_feature_no", businessTypeSrv.GetBusinessType("ItemFeature"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("ItemFeature") });
            //库位编号
            defaultType.RegisterSimpleProperty("warehouse_no", businessTypeSrv.GetBusinessType("WarehouseCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("WarehouseCode") });
            //储位编号
            defaultType.RegisterSimpleProperty("storage_spaces_no", businessTypeSrv.GetBusinessType("Bin"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("Bin") });
            //批号
            defaultType.RegisterSimpleProperty("lot_no", businessTypeSrv.GetBusinessType("LotCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("LotCode") });
            //盘点数量
            defaultType.RegisterSimpleProperty("qty", businessTypeSrv.GetBusinessType("Quantity"),
                                               0M, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("Quantity") });
            //盘点人员
            defaultType.RegisterSimpleProperty("employee_no", businessTypeSrv.GetBusinessType("BusinessCode"),
                                               string.Empty, false, new Attribute[] { businessTypeSrv.GetSimplePropertyAttribute("BusinessCode") });
            //盘点日期
            defaultType.RegisterSimpleProperty("complete_date", typeof(DateTime),
                                               OrmDataOption.EmptyDateTime, false, new Attribute[] { new SimplePropertyAttribute(GeneralDBType.Date) });
            //交易对象类型
            defaultType.RegisterSimpleProperty("transaction_type", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            tempAttr.Size = 10;
            //交易对象编号
            defaultType.RegisterSimpleProperty("transaction_no", typeof(string),
                                               string.Empty, false, new Attribute[] { tempAttr });
            #endregion
            _Table_scan = defaultType;
            _qurService.CreateTempTable(_Table_scan);
            #endregion
        }

        private void InsertToScan(string site_no, DependencyObjectCollection scan) {
            DataTable dtScan = CreateDtForScanBulk();
            IPrimaryKeyService prikeyService = this.GetService<IPrimaryKeyService>("PURCHASE_ARRIVAL");
            foreach (DependencyObject scanObj in scan) {
                DataRow drNew = dtScan.NewRow();
                foreach (DataColumn dc in dtScan.Columns) {
                    if (dc.ColumnName == "complete_date" && Maths.IsEmpty(scanObj[dc.ColumnName])) {
                        drNew[dc.ColumnName] = OrmDataOption.EmptyDateTime;
                    } else if (dc.ColumnName == "site_no") {
                        drNew[dc.ColumnName] = site_no;
                    } else {
                        drNew[dc.ColumnName] = scanObj[dc.ColumnName];
                    }
                }
                dtScan.Rows.Add(drNew);
            }
            List<BulkCopyColumnMapping> dtScanMapping = GetBulkCopyColumnMapping(dtScan.Columns);
            _qurService.BulkCopy(dtScan, _Table_scan.Name, dtScanMapping.ToArray());
        }

        /// <summary>
        /// bulkcopy需要datatable传入
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDtForScanBulk() {
            DataTable dt = new DataTable();
            dt.Columns.Add(new DataColumn("site_no", typeof(string)));
            dt.Columns.Add(new DataColumn("barcode_no", typeof(string)));
            dt.Columns.Add(new DataColumn("item_no", typeof(string)));
            dt.Columns.Add(new DataColumn("item_feature_no", typeof(string)));
            dt.Columns.Add(new DataColumn("warehouse_no", typeof(string)));
            dt.Columns.Add(new DataColumn("storage_spaces_no", typeof(string)));
            dt.Columns.Add(new DataColumn("lot_no", typeof(string)));
            dt.Columns.Add(new DataColumn("qty", typeof(decimal)));
            dt.Columns.Add(new DataColumn("employee_no", typeof(string)));
            dt.Columns.Add(new DataColumn("complete_date", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("transaction_type", typeof(string)));
            dt.Columns.Add(new DataColumn("transaction_no", typeof(string)));
            return dt;
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

        #endregion

        /// <summary>
        /// QuerySum+新增实体的查询逻辑
        /// </summary>
        /// <param name="countingNo"></param>
        /// <returns></returns>
        private QueryNode QuerySumForInsert(string countingNo, QueryNode queryScan) {
            List<QueryProperty> groupList = new List<QueryProperty>();
            List<string> paraStringList = GetSumAboutString();
            foreach (string paraStr in paraStringList) {
                groupList.Add(OOQL.CreateProperty("queryScan." + paraStr));
            }
            List<QueryProperty> selectList = new List<QueryProperty>();
            selectList.AddRange(groupList);
            selectList.Add(Formulas.Sum("queryScan.qty", "QTY"));
            QueryNode querySum = OOQL.Select(selectList.ToArray())
                .From(queryScan, "queryScan")
                .GroupBy(groupList.ToArray());
            #region  为新增COUNTING的字段进行查询
            List<QueryProperty> returnSelectList = new List<QueryProperty>();
            foreach (string paraStr in paraStringList) {
                returnSelectList.Add(OOQL.CreateProperty("querySum." + paraStr));
            }
            returnSelectList.Add(OOQL.CreateProperty("querySum.QTY"));
            returnSelectList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "ITEM_DESCRIPTION"));
            returnSelectList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"));
            returnSelectList.Add(Formulas.Cast(Formulas.Ext("UNIT_CONVERT", string.Empty, new object[]{OOQL.CreateProperty("querySum.ITEM_ID"),
                                                                  OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                                                  OOQL.CreateProperty("querySum.QTY"),
                                                                  OOQL.CreateProperty ("ITEM.SECOND_UNIT_ID"),
                                                                  OOQL.CreateConstants(0)
                        }), GeneralDBType.Decimal, 16, 6, "SECOND_QTY"));
            returnSelectList.Add(Formulas.IsNull(OOQL.CreateProperty("COUNTING_PLAN.PLAN_DATE"), OOQL.CreateConstants(DateTime.Now), "PLAN_DATE"));
            returnSelectList.Add(OOQL.CreateProperty("PLANT.COMPANY_ID"));
            returnSelectList.Add(Formulas.IsNull(OOQL.CreateProperty("COUNTING_PLAN.COUNTING_PLAN_ID"), OOQL.CreateConstants(Guid.NewGuid()), "COUNTING_PLAN_ID"));
            returnSelectList.Add(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"));
            returnSelectList.Add(OOQL.CreateProperty("COUNTING_PLAN.SequenceNumber"));
            #endregion
            return OOQL.Select(returnSelectList.ToArray())
                .From(querySum, "querySum")
                .LeftJoin("COUNTING_PLAN", "COUNTING_PLAN")
                .On(OOQL.CreateProperty("querySum.COUNTING_PLAN_ID") == OOQL.CreateProperty("COUNTING_PLAN.COUNTING_PLAN_ID"))
                .InnerJoin("ITEM", "ITEM")
                .On(OOQL.CreateProperty("querySum.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .InnerJoin("PLANT", "PLANT")
                .On(OOQL.CreateProperty("querySum.PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID"));
        }

        /// <summary>
        /// 因为这几字段会被3个地方公用，直接提取公共
        /// </summary>
        /// <returns></returns>
        private List<string> GetSumAboutString() {
            List<string> insertString = new List<string>();
            insertString.Add("COUNTING_PLAN_ID");
            insertString.Add("complete_date");
            insertString.Add("EMPLOYEE_ID");
            insertString.Add("PLANT_ID");
            insertString.Add("ITEM_ID");
            insertString.Add("ITEM_FEATURE_ID");
            insertString.Add("WAREHOUSE_ID");
            insertString.Add("BIN_ID");
            insertString.Add("ITEM_LOT_ID");
            insertString.Add("BO_ID_RTK");
            insertString.Add("BO_ID_ROid");
            return insertString;
        }

        private QueryNode QueryScan(string countingNo) {
            return OOQL.Select(OOQL.CreateProperty("Table_scan.barcode_no"),
                OOQL.CreateProperty("COUNTING_PLAN.COUNTING_PLAN_ID"),
                OOQL.CreateProperty("Table_scan.qty"),
                OOQL.CreateProperty("Table_scan.complete_date"),
                OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"),
                OOQL.CreateProperty("PLANT.PLANT_ID"),
                OOQL.CreateProperty("ITEM.ITEM_ID", "ITEM_ID"),
                Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "ITEM_FEATURE_ID"),
                OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),
                Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID"),
                Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "ITEM_LOT_ID"),
                OOQL.CreateProperty("Table_scan.transaction_type", "BO_ID_RTK"),
                Formulas.Case(null, OOQL.CreateConstants(Maths.GuidDefaultValue()), new CaseItem[]{
                          new CaseItem(OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("CUSTOMER", GeneralDBType.String),OOQL.CreateProperty("CUSTOMER.CUSTOMER_ID")),
                          new CaseItem(OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("SUPPLIER", GeneralDBType.String),OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID")),
                          new CaseItem(OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("EMPLOYEE", GeneralDBType.String),OOQL.CreateProperty("EMPLOYEE_02.EMPLOYEE_ID")),
                          new CaseItem(OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("OTHER_BO", GeneralDBType.String),OOQL.CreateProperty("OTHER_BO.OTHER_BO_ID"))}
                          , "BO_ID_ROid")
                )
        .From(_Table_scan.Name, "Table_scan")
        .InnerJoin("PLANT", "PLANT")
        .On(OOQL.CreateProperty("Table_scan.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
        .InnerJoin("ITEM", "ITEM")
        .On(OOQL.CreateProperty("Table_scan.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
        .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
        .On((OOQL.CreateProperty("Table_scan.item_feature_no") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE")
             & OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID")))
        .InnerJoin("WAREHOUSE", "WAREHOUSE")
        .On((OOQL.CreateProperty("Table_scan.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
             & OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
        .LeftJoin("WAREHOUSE.BIN", "BIN")
        .On((OOQL.CreateProperty("Table_scan.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE")
             & OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID")))
        .LeftJoin("ITEM_LOT", "ITEM_LOT")
        .On(OOQL.CreateProperty("Table_scan.lot_no") == OOQL.CreateProperty("ITEM_LOT.LOT_CODE")
            & OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")
            & ((OOQL.CreateProperty("Table_scan.item_feature_no") == OOQL.CreateConstants(string.Empty, GeneralDBType.String)
                & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
               | OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID")))
        .LeftJoin("EMPLOYEE", "EMPLOYEE")
        .On(OOQL.CreateProperty("Table_scan.employee_no") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE"))
        .LeftJoin("CUSTOMER", "CUSTOMER")
        .On(OOQL.CreateProperty("Table_scan.transaction_no") == OOQL.CreateProperty("CUSTOMER.CUSTOMER_CODE"))
        .LeftJoin("SUPPLIER", "SUPPLIER")
        .On(OOQL.CreateProperty("Table_scan.transaction_no") == OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE"))
        .LeftJoin("EMPLOYEE", "EMPLOYEE_02")
        .On(OOQL.CreateProperty("Table_scan.transaction_no") == OOQL.CreateProperty("EMPLOYEE_02.EMPLOYEE_CODE"))
        .LeftJoin("OTHER_BO", "OTHER_BO")
        .On(OOQL.CreateProperty("Table_scan.transaction_no") == OOQL.CreateProperty("OTHER_BO.OTHER_BO_CODE"))
        .LeftJoin("COUNTING_PLAN", "COUNTING_PLAN")
        .On(OOQL.CreateConstants(countingNo, GeneralDBType.String) == OOQL.CreateProperty("COUNTING_PLAN.DOC_NO")
            & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("COUNTING_PLAN.Owner_Org.ROid")
            & OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("COUNTING_PLAN.ITEM_ID")
            & Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())) == OOQL.CreateProperty("COUNTING_PLAN.ITEM_FEATURE_ID")
            & OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("COUNTING_PLAN.WAREHOUSE_ID")
            & Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())) == OOQL.CreateProperty("COUNTING_PLAN.BIN_ID")
            & Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())) == OOQL.CreateProperty("COUNTING_PLAN.ITEM_LOT_ID")
                //& Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE_02.EMPLOYEE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())) == OOQL.CreateProperty("COUNTING_PLAN.Owner_Emp")
            & ((OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("CUSTOMER", GeneralDBType.String)
                & OOQL.CreateProperty("CUSTOMER.CUSTOMER_ID") == OOQL.CreateProperty("COUNTING_PLAN.BO_ID.ROid"))
               | (OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("SUPPLIER", GeneralDBType.String)
                  & OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("COUNTING_PLAN.BO_ID.ROid"))
               | (OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("EMPLOYEE", GeneralDBType.String)
                  & OOQL.CreateProperty("EMPLOYEE_02.EMPLOYEE_ID") == OOQL.CreateProperty("COUNTING_PLAN.BO_ID.ROid"))
               | (OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("OTHER_BO", GeneralDBType.String)
                  & OOQL.CreateProperty("OTHER_BO.OTHER_BO_ID") == OOQL.CreateProperty("COUNTING_PLAN.BO_ID.ROid"))
               | (OOQL.CreateProperty("Table_scan.transaction_type") == OOQL.CreateConstants("OTHER", GeneralDBType.String))));
        }

        private List<DependencyObject> InsertCOUNTING(string countingType, string countingNo, DependencyObjectCollection sumColl, DependencyObjectType entityType) {
            List<DependencyObject> newList = new List<DependencyObject>();
            IPrimaryKeyService prikeyService = this.GetService<IPrimaryKeyService>("COUNTING");
            string countType = string.Empty;
            switch (countingType) {
                case "1":
                    countingType = "1";
                    break;
                case "3":
                    countingType = "2";
                    break;
                case "4":
                    countingType = "3";
                    break;
                default:
                    break;
            }
            int seqNumberValue = GetMaxSeq(countingNo) + 1;
            foreach (DependencyObject sumObj in sumColl) {
                DependencyObject newRecord = new DependencyObject(entityType);
                newRecord["COUNTING_ID"] = prikeyService.CreateId();
                newRecord["ITEM_ID"] = sumObj["ITEM_ID"];
                newRecord["ITEM_DESCRIPTION"] = sumObj["ITEM_DESCRIPTION"];
                newRecord["ITEM_FEATURE_ID"] = sumObj["ITEM_FEATURE_ID"];
                newRecord["ITEM_SPECIFICATION"] = sumObj["ITEM_SPECIFICATION"];
                newRecord["WAREHOUSE_ID"] = sumObj["WAREHOUSE_ID"];
                newRecord["BIN_ID"] = sumObj["BIN_ID"];
                newRecord["ITEM_LOT_ID"] = sumObj["ITEM_LOT_ID"];
                newRecord["COUNT_TYPE"] = countingType;
                newRecord["COUNTING_QTY"] = sumObj["QTY"];
                newRecord["SECOND_QTY"] = sumObj["SECOND_QTY"];
                newRecord["PLAN_DATE"] = sumObj["PLAN_DATE"];
                newRecord["COUNTING_DATE"] = sumObj["complete_date"];
                //20170302 add by shenbao for P001-170302001 ===begin===
                string rtk = sumObj["BO_ID_RTK"].ToStringExtension();
                if (string.IsNullOrEmpty(rtk))
                    rtk = "OTHER";
                //20170302 add by shenbao for P001-170302001  ===end===
                ((DependencyObject)newRecord["BO_ID"])["RTK"] = rtk;  //20170302 modi by shenbao for P001-170302001  sumObj["BO_ID_RTK"]==>rtk
                ((DependencyObject)newRecord["BO_ID"])["ROid"] = sumObj["BO_ID_ROid"];
                newRecord["COMPANY_ID"] = sumObj["COMPANY_ID"];
                newRecord["COUNTING_PLAN_ID"] = sumObj["COUNTING_PLAN_ID"];
                newRecord["DOC_NO"] = countingNo;
                newRecord["BUSINESS_UNIT_ID"] = sumObj["STOCK_UNIT_ID"];
                newRecord["BUSINESS_QTY"] = sumObj["QTY"];
                if (sumObj["SequenceNumber"] == null || sumObj["SequenceNumber"].ToInt32() <= 0) {
                    newRecord["SequenceNumber"] = seqNumberValue;
                    seqNumberValue++;
                } else {
                    newRecord["SequenceNumber"] = sumObj["SequenceNumber"];
                }
                ((DependencyObject)newRecord["Owner_Org"])["RTK"] = "PLANT";
                ((DependencyObject)newRecord["Owner_Org"])["ROid"] = sumObj["PLANT_ID"];
                newList.Add(newRecord);
            }
            return newList;
        }

        /// <summary>
        /// 查询是否启用条码库存管理/依单据生成条码
        /// </summary>
        /// <returns></returns>
        private bool GetBcInventoryManagement() {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("PARA_FIL.BC_INVENTORY_MANAGEMENT"))
                                                    .From("PARA_FIL", "PARA_FIL")
                                                    .Where(OOQL.AuthFilter("PARA_FIL", "PARA_FIL"));
            return this.GetService<IQueryService>().ExecuteScalar(node).ToBoolean();
        }

        private QueryNode GetUpdateNode(string counting_type, string countingNo, QueryNode querySumBarcodeNode) {
            return OOQL.Update("COUNTING_PLAN_BARCODE")
                      .Set(new SetItem[] { new SetItem(OOQL.CreateProperty("COUNTING_QTY"), Formulas.Case(null,OOQL.CreateProperty("querySumBarcode.QTY"),new CaseItem[]{
                                                    new CaseItem(OOQL.CreateProperty("COUNTING_PLAN_BARCODE.COUNTING_TYPE")==OOQL.CreateConstants(counting_type),
                                                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.COUNTING_QTY")+OOQL.CreateProperty("querySumBarcode.QTY"))})),
                                            new SetItem(OOQL.CreateProperty("COUNTING_TYPE"),OOQL.CreateConstants(counting_type))})
                    .From(querySumBarcodeNode, "querySumBarcode")
                    .Where(OOQL.CreateProperty("COUNTING_PLAN_BARCODE.DOC_NO") == OOQL.CreateConstants(countingNo) &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.BARCODE_NO") == OOQL.CreateProperty("querySumBarcode.barcode_no") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.PLANT_ID") == OOQL.CreateProperty("querySumBarcode.PLANT_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.ITEM_ID") == OOQL.CreateProperty("querySumBarcode.ITEM_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.ITEM_FEATURE_ID") == OOQL.CreateProperty("querySumBarcode.ITEM_FEATURE_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.WAREHOUSE_ID") == OOQL.CreateProperty("querySumBarcode.WAREHOUSE_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.BIN_ID") == OOQL.CreateProperty("querySumBarcode.BIN_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.ITEM_LOT_ID") == OOQL.CreateProperty("querySumBarcode.ITEM_LOT_ID")
                //20170505 mark by shenbao for P001-170505001 ===begin===
                    //OOQL.CreateProperty("COUNTING_PLAN_BARCODE.BO_ID.ROid") == OOQL.CreateProperty("querySumBarcode.BO_ID_ROid") &
                    //OOQL.CreateProperty("COUNTING_PLAN_BARCODE.BO_ID.RTK") == OOQL.CreateProperty("querySumBarcode.BO_ID_RTK")
                //20170505 mark by shenbao for P001-170505001 ===end===
                    );
        }
        /// <summary>
        /// QuerySumBarcode
        /// </summary>
        /// <param name="countingNo"></param>
        /// <param name="queryScan"></param>
        /// <returns></returns>
        private QueryNode QuerySumBarcode(string countingNo, QueryNode queryScan) {
            List<QueryProperty> groupList = new List<QueryProperty>();
            List<string> paraStringList = GetSumAboutString();
            foreach (string paraStr in paraStringList) {
                if (paraStr != "COUNTING_PLAN_ID") {//20170324 add by wangyq for B001-170324010
                    groupList.Add(OOQL.CreateProperty("queryScan." + paraStr));
                }//20170324 add by wangyq for B001-170324010
            }
            groupList.Add(OOQL.CreateProperty("queryScan.barcode_no"));
            List<QueryProperty> selectList = new List<QueryProperty>();
            selectList.AddRange(groupList);
            selectList.Add(Formulas.Sum("queryScan.qty", "QTY"));
            return OOQL.Select(selectList.ToArray())
                  .From(queryScan, "queryScan")
                  .GroupBy(groupList.ToArray());
        }

        private QueryNode QueryNodeForInsert(string countingNo, string countingType, QueryNode sumBarCodeNode) {
            QueryNode selectNode = OOQL.Select(Formulas.NewId("COUNTING_PLAN_BARCODE_ID"),
                                               OOQL.CreateConstants(countingNo),
                                               OOQL.CreateConstants(countingType),
                                               OOQL.CreateProperty("sumBarCode.barcode_no"),
                                               OOQL.CreateProperty("sumBarCode.ITEM_ID"),
                                               OOQL.CreateProperty("sumBarCode.ITEM_FEATURE_ID"),
                                               OOQL.CreateProperty("sumBarCode.WAREHOUSE_ID"),
                                               OOQL.CreateProperty("sumBarCode.BIN_ID"),
                                               OOQL.CreateProperty("sumBarCode.ITEM_LOT_ID"),
                                               OOQL.CreateProperty("sumBarCode.QTY"),
                                               OOQL.CreateProperty("sumBarCode.PLANT_ID"),
                                               OOQL.CreateConstants("OTHER", GeneralDBType.String, "BO_ID_RTK"),
                                               OOQL.CreateConstants(Maths.GuidDefaultValue(), GeneralDBType.Guid, "BO_ID_ROid")
                                               )
                .From(sumBarCodeNode, "sumBarCode")
                .LeftJoin("COUNTING_PLAN_BARCODE", "COUNTING_PLAN_BARCODE")
                .On(OOQL.CreateProperty("COUNTING_PLAN_BARCODE.DOC_NO") == OOQL.CreateConstants(countingNo) &
                  OOQL.CreateProperty("COUNTING_PLAN_BARCODE.BARCODE_NO") == OOQL.CreateProperty("sumBarCode.barcode_no") &
                  OOQL.CreateProperty("COUNTING_PLAN_BARCODE.PLANT_ID") == OOQL.CreateProperty("sumBarCode.PLANT_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.ITEM_ID") == OOQL.CreateProperty("sumBarCode.ITEM_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.ITEM_FEATURE_ID") == OOQL.CreateProperty("sumBarCode.ITEM_FEATURE_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.WAREHOUSE_ID") == OOQL.CreateProperty("sumBarCode.WAREHOUSE_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.BIN_ID") == OOQL.CreateProperty("sumBarCode.BIN_ID") &
                    OOQL.CreateProperty("COUNTING_PLAN_BARCODE.ITEM_LOT_ID") == OOQL.CreateProperty("sumBarCode.ITEM_LOT_ID")
                //20170505 mark by shenbao for P001-170505001 ===begin===
                    //OOQL.CreateProperty("COUNTING_PLAN_BARCODE.BO_ID.ROid") == OOQL.CreateProperty("sumBarCode.BO_ID_ROid") &
                    //OOQL.CreateProperty("COUNTING_PLAN_BARCODE.BO_ID.RTK") == OOQL.CreateProperty("sumBarCode.BO_ID_RTK")
                //20170505 mark by shenbao for P001-170505001 ===end===
                  )
                .InnerJoin("ITEM", "ITEM")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("sumBarCode.ITEM_ID"))
                .Where(OOQL.CreateProperty("COUNTING_PLAN_BARCODE.COUNTING_PLAN_BARCODE_ID").IsNull());
            List<string> insertString = new List<string>();
            insertString.Add("COUNTING_PLAN_BARCODE_ID");
            insertString.Add("DOC_NO");
            insertString.Add("COUNTING_TYPE");
            insertString.Add("BARCODE_NO");
            insertString.Add("ITEM_ID");
            insertString.Add("ITEM_FEATURE_ID");
            insertString.Add("WAREHOUSE_ID");
            insertString.Add("BIN_ID");
            insertString.Add("ITEM_LOT_ID");
            insertString.Add("COUNTING_QTY");
            insertString.Add("PLANT_ID");
            insertString.Add("BO_ID.RTK");
            insertString.Add("BO_ID.ROid");
            return OOQL.Insert("COUNTING_PLAN_BARCODE", selectNode, insertString.ToArray());
        }

        /// <summary>
        /// 根据单号获取最大序号
        /// </summary>
        /// <param name="countingNo"></param>
        /// <returns></returns>
        private int GetMaxSeq(string countingNo) {
            QueryNode node = OOQL.Select(Formulas.Max(OOQL.CreateProperty("SequenceNumber"), "SequenceNumber"))
                .From("COUNTING_PLAN", "COUNTING_PLAN")
                .Where(OOQL.CreateProperty("DOC_NO") == OOQL.CreateConstants(countingNo));
            return this.GetService<IQueryService>().ExecuteScalar(node).ToInt32();
        }

        /// <summary>
        /// 忽略保存服务的警告校验
        /// </summary>
        /// <param name="isIgnore"></param>
        private void SetIgnoreWarningTag(bool isIgnore) {
            DeliverContext deliver = CallContext.GetData(DeliverContext.Name) as DeliverContext;
            if (deliver == null) {
                deliver = new DeliverContext();
                deliver.Add("IgnoreWarning", isIgnore);
                CallContext.SetData(DeliverContext.Name, deliver);
            } else {
                if (deliver.ContainsKey("IgnoreWarning")) {
                    deliver["IgnoreWarning"] = isIgnore;
                } else {
                    deliver.Add("IgnoreWarning", isIgnore);
                }
            }
        }
    }
}
