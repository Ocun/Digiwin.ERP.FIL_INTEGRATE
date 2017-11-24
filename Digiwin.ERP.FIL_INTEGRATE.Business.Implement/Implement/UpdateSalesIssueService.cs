//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/9/5 14:27:33</CreateDate>
//<IssueNO>P001-170717001</IssueNO>
//<Description>更新销货出库单服务</Description>
//----------------------------------------------------------------  

using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using System;
using Digiwin.Common.Torridity.Metadata;
using System.Data;
using System.Collections.Generic;
using Digiwin.ERP.Common.Utils;
using Digiwin.Common.Core;
using Digiwin.ERP.Common.Business;
using Digiwin.Common.Services;
using System.Linq;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    /// <summary>
    ///  
    /// </summary>
    [SingleGetCreator]
    [ServiceClass(typeof(IUpdateSalesIssueService))]
    [Description("")]
    sealed class UpdateSalesIssueService : ServiceComponent, IUpdateSalesIssueService {

        public DependencyObjectCollection UpdateSalesIssue(string employeeNo, string scanType, DateTime reportDatetime, string pickingDepartmentNo
            , string recommendedOperations, string recommendedFunction, string scanDocNo, DependencyObjectCollection collection) {
            DependencyObjectCollection rtnColl = CreateReturnCollection();
            #region 参数检查
            IInfoEncodeContainer encodeSrv = this.GetService<IInfoEncodeContainer>();
            if (Maths.IsEmpty(recommendedOperations)) {
                throw new BusinessRuleException(encodeSrv.GetMessage("A111201", new object[] { "recommended_operations" }));
            }
            if (Maths.IsEmpty(recommendedFunction)) {
                throw new BusinessRuleException(encodeSrv.GetMessage("A111201", new object[] { "recommended_function" }));
            }
            #endregion

            //创建临时表需要的DataTable和Mapping信息
            DataTable dtEntityD = new DataTable();
            List<BulkCopyColumnMapping> entityDMap = new List<BulkCopyColumnMapping>();
            CreateRelateTable(dtEntityD, entityDMap);
            List<string> docNos = new List<string>();  //记录单身的单号，保存时需要，可以避免后续再查

            //组织数据BulkCopy需要的DataTable数据
            InsertDataTable(dtEntityD, collection, docNos);
            if (dtEntityD.Rows.Count <= 0) {
                return rtnColl;
            }

            #region 更新逻辑
            using (ITransactionService trans = this.GetService<ITransactionService>()) {
                using (IConnectionService connService = this.GetService<IConnectionService>()) {
                    IQueryService querySrv = this.GetService<IQueryService>();
                    //新增临时表
                    IDataEntityType dtTemp = CreateDTmpTable(querySrv);

                    //批量新增到临时表
                    querySrv.BulkCopy(dtEntityD, dtTemp.Name, entityDMap.ToArray());
                    //利用临时表批量更新相关数据
                    UpdateSalesIssueD(querySrv, dtTemp);

                    DataRow[] drs = dtEntityD.Select("BARCODE_NO<>\'\'");
                    if (drs.Length > 0) {  //条码不为""的记录大于0时，才执行如下更新，避免多余的性能损失
                        ICreateService createService = this.GetService<ICreateService>("PARA_FIL");
                        if (createService != null) {//PARA_FIL实体存在,服务端可以这么判断
                            bool bcLintFlag = UtilsClass.IsBCLineManagement(querySrv);
                            if (bcLintFlag) {
                                DeleteBCLine(querySrv, dtTemp);  //先删除
                                InsertBCLine(querySrv, dtTemp);  //再重新生成
                            }
                        }
                    }
                }

                //保存
                DependencyObjectCollection ids = GetSalesIssue(docNos);
                IReadService readSrv = this.GetService<IReadService>("SALES_ISSUE");
                object[] entities = readSrv.Read(ids.Select(c => c["SALES_ISSUE_ID"]).ToArray());
                if (entities != null && entities.Length > 0) {
                    ISaveService saveSrv = this.GetService<ISaveService>("SALES_ISSUE");
                    saveSrv.Save(entities);
                }

                //保存时没有自动审核的，需要重新审核
                entities = readSrv.Read(ids.Where(c => !c["AUTO_APPROVE"].ToBoolean()).Select(c => c["SALES_ISSUE_ID"]).ToArray());
                IConfirmService confirmService = this.GetService<IConfirmService>("SALES_ISSUE");
                ILogOnService logOnSrv = this.GetService<ILogOnService>();
                foreach (DependencyObject obj in entities) {
                    ConfirmContext context = new ConfirmContext(obj.Oid, logOnSrv.CurrentUserId, reportDatetime.ToDate());
                    confirmService.Execute(context);
                }

                trans.Complete();
            }
            #endregion

            #region 组织返回结果

            foreach (string item in docNos) {
                DependencyObject obj = rtnColl.AddNew();
                obj["doc_no"] = item;
            }

            #endregion

            return rtnColl;
        }
        /// <summary>
        /// 创建服务返回集合
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection CreateReturnCollection() {
            DependencyObjectType type = new DependencyObjectType("ReturnCollection");
            type.RegisterSimpleProperty("doc_no", typeof(string));

            DependencyObjectCollection Rtn = new DependencyObjectCollection(type);
            return Rtn;
        }

        /// <summary>
        /// 创建批量修改所需要的DataTable和Mapping
        /// </summary>
        private void CreateRelateTable(DataTable issueReceiptD, List<BulkCopyColumnMapping> issueReceiptDMap) {
            issueReceiptD.Columns.Add("item_no", typeof(string));
            issueReceiptD.Columns.Add("item_feature_no", typeof(string));
            issueReceiptD.Columns.Add("picking_unit_no", typeof(string));
            issueReceiptD.Columns.Add("doc_no", typeof(string));
            issueReceiptD.Columns.Add("seq", typeof(Int32));
            issueReceiptD.Columns.Add("warehouse_no", typeof(string));
            issueReceiptD.Columns.Add("storage_spaces_no", typeof(string));
            issueReceiptD.Columns.Add("lot_no", typeof(string));
            issueReceiptD.Columns.Add("picking_qty", typeof(decimal));
            issueReceiptD.Columns.Add("barcode_no", typeof(string));

            //创建map对照表
            Dictionary<string, string> dicIssueReceiptD = new Dictionary<string, string>();
            foreach (DataColumn dc in issueReceiptD.Columns) {
                issueReceiptDMap.Add(new BulkCopyColumnMapping(dc.ColumnName, dc.ColumnName));
            }
        }

        /// <summary>
        /// 组织临时表准备数据
        /// </summary>
        /// <param name="dtEntityD"></param>
        /// <param name="colls"></param>
        /// <param name="docNos"></param>
        private void InsertDataTable(DataTable dtEntityD, DependencyObjectCollection colls, List<string> docNos) {
            foreach (DependencyObject obj in colls) {
                DependencyObjectCollection subColls = obj["scan_detail"] as DependencyObjectCollection;
                if (subColls != null && subColls.Count > 0) {
                    foreach (DependencyObject subObj in subColls) {
                        DataRow dr = dtEntityD.NewRow();
                        string docNo = subObj["doc_no"].ToStringExtension();
                        foreach (DataColumn dt in dtEntityD.Columns) {
                            dr[dt.ColumnName] = subObj[dt.ColumnName];
                        }
                        dtEntityD.Rows.Add(dr);
                        //记录单号，后续保存和审核需要使用
                        if (docNo != string.Empty && !docNos.Contains(docNo)) {
                            docNos.Add(docNo);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 存储所需修改IssueReceiptD的数据集合的临时表
        /// </summary>
        private IDataEntityType CreateDTmpTable(IQueryService qrySrv) {
            string typeName = "Temp_UpdateSalesIssueD_" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });
            IBusinessTypeService businessSrv = this.GetServiceForThisTypeKey<IBusinessTypeService>();

            #region 字段

            defaultType.RegisterSimpleProperty("item_no", businessSrv.SimpleItemCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleItemCode });

            defaultType.RegisterSimpleProperty("item_feature_no", businessSrv.SimpleItemFeatureType, string.Empty, false, new Attribute[] { businessSrv.SimpleItemFeature });

            defaultType.RegisterSimpleProperty("picking_unit_no", businessSrv.SimpleUnitCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleUnitCode });

            defaultType.RegisterSimpleProperty("doc_no", businessSrv.SimpleDocNoType, string.Empty, false, new Attribute[] { businessSrv.SimpleDocNo });

            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("seq", typeof(Int32), 0, false, new Attribute[] { tempAttr });

            defaultType.RegisterSimpleProperty("warehouse_no", businessSrv.GetBusinessType("WarehouseCode"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("WarehouseCode") });

            defaultType.RegisterSimpleProperty("storage_spaces_no", businessSrv.GetBusinessType("Bin"), string.Empty, false, new Attribute[] { businessSrv.GetSimplePropertyAttribute("Bin") });

            defaultType.RegisterSimpleProperty("lot_no", businessSrv.SimpleLotCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleLotCode });

            defaultType.RegisterSimpleProperty("picking_qty", businessSrv.SimpleQuantityType, 0M, false, new Attribute[] { businessSrv.SimpleQuantity });

            tempAttr = new SimplePropertyAttribute(GeneralDBType.String, 1000);
            defaultType.RegisterSimpleProperty("barcode_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });

            #endregion

            qrySrv.CreateTempTable(defaultType);
            return defaultType;
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量更新领料单
        /// </summary>
        private void UpdateSalesIssueD(IQueryService qrySrv, IDataEntityType tempEntityD) {
            QueryNode selectNode = OOQL.Select(true, OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_D_ID"))
                .From("SALES_ISSUE.SALES_ISSUE_D", "SALES_ISSUE_D")
                .InnerJoin("SALES_ISSUE", "SALES_ISSUE")
                .On(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID") == OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_ID"))
                .InnerJoin(tempEntityD.Name, "TEMP")
                .On(OOQL.CreateProperty("TEMP.doc_no") == OOQL.CreateProperty("SALES_ISSUE.DOC_NO")
                & OOQL.CreateProperty("TEMP.seq") == OOQL.CreateProperty("SALES_ISSUE_D.SequenceNumber"));

            QueryNode node = OOQL.Update("SALES_ISSUE.SALES_ISSUE_D")
                .Set(new SetItem[] { new SetItem(OOQL.CreateProperty("BC_CHECK_STATUS"), OOQL.CreateConstants("2")) })
                .From(selectNode, "selectNode")
                .Where(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_D.SALES_ISSUE_D_ID") == OOQL.CreateProperty("selectNode.SALES_ISSUE_D_ID"));
            qrySrv.ExecuteNoQueryWithManageProperties(node);
        }

        /// <summary>
        /// 先删除BCLine，后面再重新生成
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpIssueReceiptD"></param>
        private void DeleteBCLine(IQueryService qrySrv, IDataEntityType tempEntityD) {
            QueryNode deleteNode = OOQL.Delete("BC_LINE")
                .Where(OOQL.CreateProperty("SOURCE_ID.ROid").In(
                    OOQL.Select(OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_D_ID"))
                    .From("SALES_ISSUE", "SALES_ISSUE")
                    .InnerJoin(tempEntityD.Name, "TMP")
                    .On(OOQL.CreateProperty("SALES_ISSUE.DOC_NO") == OOQL.CreateProperty("TMP.doc_no"))
                    .InnerJoin("SALES_ISSUE.SALES_ISSUE_D", "SALES_ISSUE_D")
                    .On(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID") == OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_ID"))
                ));
            qrySrv.ExecuteNoQuery(deleteNode);
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增条码交易明细
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpBCLine"></param>
        private void InsertBCLine(IQueryService qrySrv, IDataEntityType tmpIssueReceiptD) {
            #region properties
            List<QueryProperty> selectListPro = new List<QueryProperty>();

            selectListPro.Add(Formulas.NewId("BC_LINE_ID"));  //主键
            selectListPro.Add(OOQL.CreateProperty("TmpTable.barcode_no", "BARCODE_NO")); //条码CODE
            selectListPro.Add(OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D", "SOURCE_ID.RTK"));  //来源单据类型
            selectListPro.Add(OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_D_ID", "SOURCE_ID.ROid"));  //来源单据
            selectListPro.Add(Formulas.Ext("UNIT_CONVERT", "QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("TmpTable.sum_picking_qty")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}));//数量
            selectListPro.Add(Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID")); //仓库  
            selectListPro.Add(Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("BIN.BIN_ID"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("TmpTable.storage_spaces_no")==OOQL.CreateConstants(string.Empty)
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID"));  //库位 

            #endregion

            QueryNode groupNode = GetGroupForInsert(tmpIssueReceiptD);
            QueryNode insertNode = OOQL.Select(selectListPro.ToArray())
                .From(groupNode, "TmpTable")
                .InnerJoin("SALES_ISSUE", "SALES_ISSUE")
                .On(OOQL.CreateProperty("TmpTable.doc_no") == OOQL.CreateProperty("SALES_ISSUE.DOC_NO"))
                .InnerJoin("SALES_ISSUE.SALES_ISSUE_D", "SALES_ISSUE_D")
                .On(OOQL.CreateProperty("TmpTable.seq") == OOQL.CreateProperty("SALES_ISSUE_D.SequenceNumber")
                    & OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID") == OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_ID"))
                .InnerJoin("ITEM", "ITEM")
                .On(OOQL.CreateProperty("TmpTable.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .InnerJoin("UNIT", "UNIT")
                .On(OOQL.CreateProperty("TmpTable.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                .LeftJoin("WAREHOUSE", "WAREHOUSE")
                .On(OOQL.CreateProperty("TmpTable.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                    & OOQL.CreateProperty("SALES_ISSUE.Owner_Org.ROid") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("TmpTable.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE"))
                .Where(OOQL.CreateProperty("TmpTable.barcode_no") != OOQL.CreateConstants(String.Empty));

            InsertBcLineDB(insertNode, qrySrv);
        }

        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="tmpIssueReceiptD"></param>
        /// <returns></returns>
        public QueryNode GetGroupForInsert(IDataEntityType tempEntityD) {
            List<QueryProperty> selectOrGroupList = new List<QueryProperty>();
            selectOrGroupList.Add(OOQL.CreateProperty("item_no"));
            selectOrGroupList.Add(OOQL.CreateProperty("item_feature_no"));
            selectOrGroupList.Add(OOQL.CreateProperty("picking_unit_no"));
            selectOrGroupList.Add(OOQL.CreateProperty("doc_no"));
            selectOrGroupList.Add(OOQL.CreateProperty("seq"));
            selectOrGroupList.Add(OOQL.CreateProperty("warehouse_no"));
            selectOrGroupList.Add(OOQL.CreateProperty("storage_spaces_no"));
            selectOrGroupList.Add(OOQL.CreateProperty("lot_no"));
            selectOrGroupList.Add(OOQL.CreateProperty("barcode_no"));

            List<QueryProperty> selectList = new List<QueryProperty>();
            foreach (QueryProperty selectOrGroupObj in selectOrGroupList) {
                selectList.Add(selectOrGroupObj);
            }
            selectList.Add(Formulas.Sum(OOQL.CreateProperty("picking_qty"), "sum_picking_qty"));

            QueryNode selectNode = OOQL.Select(selectList.ToArray())
                 .From(tempEntityD.Name, "TEMP")
                 .GroupBy(selectOrGroupList.ToArray());
            return selectNode;
        }

        /// <summary>
        /// 获取新插入的领料单，重新保存
        /// </summary>
        /// <param name="docNos">单号集合</param>
        /// <returns></returns>
        private DependencyObjectCollection GetSalesIssue(List<string> docNos) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID"),
                                         OOQL.CreateProperty("DOC.AUTO_APPROVE"))
                .From("SALES_ISSUE", "SALES_ISSUE")
                .InnerJoin("DOC", "DOC")
                .On(OOQL.CreateProperty("SALES_ISSUE.DOC_ID") == OOQL.CreateProperty("DOC.DOC_ID"))
                .Where(OOQL.AuthFilter("SALES_ISSUE", "SALES_ISSUE") &
                OOQL.CreateProperty("SALES_ISSUE.DOC_NO").In(OOQL.CreateDyncParameter("docnos", docNos.ToArray())));
            return this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 新增到数据库
        /// </summary>
        /// <param name="insertNode"></param>
        /// <param name="qurService"></param>
        private void InsertBcLineDB(QueryNode insertNode, IQueryService qurService) {
            List<string> insertList = new List<string>();
            insertList.Add("BC_LINE_ID");
            insertList.Add("BARCODE_NO");
            insertList.Add("SOURCE_ID.RTK");
            insertList.Add("SOURCE_ID.ROid");
            insertList.Add("QTY");
            insertList.Add("WAREHOUSE_ID");
            insertList.Add("BIN_ID");
            QueryNode node = OOQL.Insert("BC_LINE", insertNode, insertList.ToArray());
            qurService.ExecuteNoQueryWithManageProperties(node);
        }
    }
}
