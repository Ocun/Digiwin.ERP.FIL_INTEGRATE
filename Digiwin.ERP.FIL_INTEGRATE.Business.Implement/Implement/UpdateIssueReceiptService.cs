//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/08 13:19:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>更新领退料单服务实现</description>
//----------------------------------------------------------------  
//20161208 modi by shenbao fro P001-161208001
//20170905 modi by wangyq for P001-170717001  

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Business;
using System.Data;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Core;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [SingleGetCreator]
    [ServiceClass(typeof(IUpdateIssueReceiptService))]
    [Description("更新领退料单服务实现")]
    public sealed class UpdateIssueReceiptService : ServiceComponent, IUpdateIssueReceiptService {
        #region 相关服务

        private IInfoEncodeContainer _encodeSrv;
        /// <summary>
        /// 信息编码服务
        /// </summary>
        public IInfoEncodeContainer EncodeSrv {
            get {
                if (_encodeSrv == null)
                    _encodeSrv = this.GetService<IInfoEncodeContainer>();

                return _encodeSrv;
            }
        }

        #endregion

        #region IUpdateIssueReceiptService 成员

        /// <summary>
        /// 更新领退料单
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型 1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collection">接口传入的领料单单身数据集合</param>
        public DependencyObjectCollection UpdateIssueReceipt(string employeeNo, string scanType, DateTime reportDatetime, string pickingDepartmentNo
            , string recommendedOperations, string recommendedFunction, string scanDocNo, DependencyObjectCollection collection) {
            DependencyObjectCollection rtnColl = CreateReturnCollection();
            #region 参数检查
            if (Maths.IsEmpty(recommendedOperations)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "recommended_operations" }));
            }
            if (Maths.IsEmpty(recommendedFunction)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "recommended_function" }));
            }
            #endregion

            //创建临时表需要的DataTable和Mapping信息
            DataTable issueReceiptD = null;
            List<BulkCopyColumnMapping> issueReceiptDMap = null;
            this.CreateRelateTable(ref issueReceiptD,
                ref issueReceiptDMap);
            List<string> docNos = new List<string>();  //记录单身的单号，保存时需要，可以避免后续再查

            //组织数据BulkCopy需要的DataTable数据
            InsertDataTable(issueReceiptD, collection, docNos);

            #region 更新逻辑
            using (ITransactionService trans = this.GetService<ITransactionService>()) {
                IQueryService querySrv = this.GetService<IQueryService>();

                //新增临时表
                IDataEntityType issueReceiptDTmp = CreateIssueReceiptDTmpTable(querySrv);

                //批量新增到临时表
                querySrv.BulkCopy(issueReceiptD, issueReceiptDTmp.Name, issueReceiptDMap.ToArray());
                if (issueReceiptD.Rows.Count <= 00 || issueReceiptD.Rows.Count <= 0)  //没有数据值不再往下执行
                    return rtnColl;
                DataRow[] drs = issueReceiptD.Select("BARCODE_NO<>\'\'");

                //利用临时表批量更新相关数据
                //20170905 add by wangyq for P001-170717001  =================begin===================
                if (recommendedOperations == "7-5") {
                    UpdateBcCheckStatus(querySrv, issueReceiptDTmp);
                } else {
                    //20170905 add by wangyq for P001-170717001  =================end===================
                    UpdateIssueReceiptD(querySrv, issueReceiptDTmp);
                }//20170905 add by wangyq for P001-170717001 
                if (drs.Length > 0)  //条码不为""的记录大于0时，才执行如下更新，避免多余的性能损失
                {
                    bool bcLintFlag = UtilsClass.IsBCLineManagement(querySrv);
                    if (bcLintFlag) {
                        DeleteBCLine(querySrv, issueReceiptDTmp);  //先删除
                        InsertBCLine(querySrv, issueReceiptDTmp);  //再重新生成
                    }
                }

                //保存
                DependencyObjectCollection ids = GetIssueReceipt(docNos);
                IReadService readSrv = this.GetService<IReadService>("ISSUE_RECEIPT");
                object[] entities = readSrv.Read(ids.Select(c => c["ISSUE_RECEIPT_ID"]).ToArray());
                if (entities != null && entities.Length > 0) {
                    ISaveService saveSrv = this.GetService<ISaveService>("ISSUE_RECEIPT");
                    saveSrv.Save(entities);
                }

                //保存时没有自动审核的，需要重新审核
                entities = readSrv.Read(ids.Where(c => !c["AUTO_APPROVE"].ToBoolean()).Select(c => c["ISSUE_RECEIPT_ID"]).ToArray());
                IConfirmService confirmService = this.GetService<IConfirmService>("ISSUE_RECEIPT");
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

        #endregion

        #region 自定义方法

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
        /// 组织更新IssueReceipt bcLine DataTable
        /// </summary>
        /// <param name="issueReceiptD"></param>
        private void InsertDataTable(DataTable issueReceiptD
            , DependencyObjectCollection colls, List<string> docNos) {
            foreach (DependencyObject obj in colls) {
                DependencyObjectCollection subColls = obj["scan_detail"] as DependencyObjectCollection;
                if (subColls != null && subColls.Count > 0) {
                    foreach (DependencyObject subObj in subColls) {
                        #region 新增表issueReceiptD结构
                        DataRow dr = issueReceiptD.NewRow();
                        string docNo = subObj["doc_no"].ToStringExtension();
                        dr["SequenceNumber"] = subObj["seq"].ToInt32();  //序号
                        dr["DOC_NO"] = subObj["doc_no"];  //领料单单号
                        dr["ISSUE_RECEIPT_QTY"] = subObj["picking_qty"].ToDecimal();  //领退料数量
                        dr["UNIT_CODE"] = subObj["picking_unit_no"];   //单位编号  

                        dr["WAREHOUSE_CODE"] = subObj["warehouse_no"];  //仓库编号
                        dr["BIN_CODE"] = subObj["storage_spaces_no"];  //库位编号
                        dr["LOT_CODE"] = subObj["lot_no"];  //批号

                        dr["PLANT_CODE"] = subObj["site_no"];  //工厂编号
                        dr["ITEM_CODE"] = subObj["item_no"];  //品号
                        dr["ITEM_FEATURE_CODE"] = subObj["item_feature_no"];  //特征码编号
                        dr["BARCODE_NO"] = subObj["barcode_no"];  //条码

                        issueReceiptD.Rows.Add(dr);
                        #endregion

                        //记录单号，后续保存和审核需要使用
                        if (docNo != string.Empty && !docNos.Contains(docNo)) {
                            docNos.Add(docNo);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量更新领料单
        /// </summary>
        private void UpdateIssueReceiptD(IQueryService qrySrv, IDataEntityType tmpIssueReceipt) {
            QueryNode groupNode = GroupNode(tmpIssueReceipt, true);
            #region 查询
            QueryNode selectNode = OOQL.Select(OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_D_ID")  //领料单主键
                    , OOQL.CreateProperty("TmpIssueReceipt.ISSUE_RECEIPT_QTY")  //领退料数量
                    , Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "UNIT_ID")  //单位  //20161208 modi by shenbao fro P001-161208001
                    , Formulas.Ext("UNIT_CONVERT", "SECOND_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("TmpIssueReceipt.ISSUE_RECEIPT_QTY")
                        , OOQL.CreateProperty("ITEM.SECOND_UNIT_ID")
                        , OOQL.CreateConstants(0)})  //领料第二数量
                    , Formulas.Ext("UNIT_CONVERT", "ACTUAL_SECOND_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("TmpIssueReceipt.ISSUE_RECEIPT_QTY")
                        , OOQL.CreateProperty("ITEM.SECOND_UNIT_ID")
                        , OOQL.CreateConstants(0)})  //实际第二数量
                    , Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("TmpIssueReceipt.ISSUE_RECEIPT_QTY")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)})  //领料库存数量
                    , Formulas.Ext("UNIT_CONVERT", "ACTUAL_INVENTORY_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("TmpIssueReceipt.ISSUE_RECEIPT_QTY")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)})  //实际库存数量
                    , Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID")  //仓库 //20161208 modi by shenbao fro P001-161208001
                    , Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("BIN.BIN_ID"), new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("TmpIssueReceipt.BIN_CODE")==OOQL.CreateConstants(""),
                            OOQL.CreateConstants(Maths.GuidDefaultValue()))
                    }), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID")  //库位  //20161208 modi by shenbao fro P001-161208001
                    , Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"), new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("TmpIssueReceipt.LOT_CODE")==OOQL.CreateConstants(""),
                            OOQL.CreateConstants(Maths.GuidDefaultValue()))
                    }), OOQL.CreateConstants(Maths.GuidDefaultValue()), "ITEM_LOT_ID")  //批号  //20161208 modi by shenbao fro P001-161208001
                    , Formulas.Case(null, OOQL.CreateConstants("COST_DOMAIN"), new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(1),
                            OOQL.CreateConstants("COMPANY"))
                    }, "COST_DOMAIN_ID_RTK")  //成本域类型
                    , Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("WAREHOUSE.COST_DOMAIN_ID"), new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(1),
                            OOQL.CreateProperty("PLANT.PLANT_ID")),
                        new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(2),
                            OOQL.CreateProperty("PLANT.COST_DOMAIN_ID"))
                    }), OOQL.CreateConstants(Maths.GuidDefaultValue()), "COST_DOMAIN_ID_ROid")  //成本域  //20161208 modi by shenbao fro P001-161208001 
                )
                .From("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                .InnerJoin("ISSUE_RECEIPT")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID"))
                .InnerJoin(groupNode, "TmpIssueReceipt")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO") == OOQL.CreateProperty("TmpIssueReceipt.DOC_NO")
                    & OOQL.CreateProperty("ISSUE_RECEIPT_D.SequenceNumber") == OOQL.CreateProperty("TmpIssueReceipt.SequenceNumber"))
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("TmpIssueReceipt.PLANT_CODE") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("TmpIssueReceipt.ITEM_CODE") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID")
                    & OOQL.CreateProperty("TmpIssueReceipt.ITEM_FEATURE_CODE") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"))
                .InnerJoin("UNIT")
                .On(OOQL.CreateProperty("TmpIssueReceipt.UNIT_CODE") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("TmpIssueReceipt.WAREHOUSE_CODE") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                    & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("TmpIssueReceipt.BIN_CODE") == OOQL.CreateProperty("BIN.BIN_CODE"))
                .LeftJoin("ITEM_LOT")
                .On(OOQL.CreateProperty("TmpIssueReceipt.LOT_CODE") == OOQL.CreateProperty("ITEM_LOT.LOT_CODE")
                    & OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_ID")
                    & ((OOQL.CreateProperty("TmpIssueReceipt.ITEM_FEATURE_CODE") == OOQL.CreateConstants("")
                        & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                        | (OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID"))))
                .InnerJoin("PARA_COMPANY")
                .On(OOQL.CreateProperty("PLANT.COMPANY_ID") == OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid"));
            #endregion

            #region 执行修改
            QueryNode updateNode = OOQL.Update("ISSUE_RECEIPT.ISSUE_RECEIPT_D")
                .Set(new SetItem[]{
                    new SetItem(OOQL.CreateProperty("ISSUE_RECEIPT_QTY"),OOQL.CreateProperty("SelectNode.ISSUE_RECEIPT_QTY")),
                    new SetItem(OOQL.CreateProperty("ACTUAL_ISSUE_RECEIPT_QTY"),OOQL.CreateProperty("SelectNode.ISSUE_RECEIPT_QTY")),
                    new SetItem(OOQL.CreateProperty("UNIT_ID"),OOQL.CreateProperty("SelectNode.UNIT_ID")),
                    new SetItem(OOQL.CreateProperty("SECOND_QTY"),OOQL.CreateProperty("SelectNode.SECOND_QTY")),
                    new SetItem(OOQL.CreateProperty("ACTUAL_SECOND_QTY"),OOQL.CreateProperty("SelectNode.ACTUAL_SECOND_QTY")),
                    new SetItem(OOQL.CreateProperty("INVENTORY_QTY"),OOQL.CreateProperty("SelectNode.INVENTORY_QTY")),
                    new SetItem(OOQL.CreateProperty("ACTUAL_INVENTORY_QTY"),OOQL.CreateProperty("SelectNode.ACTUAL_INVENTORY_QTY")),
                    new SetItem(OOQL.CreateProperty("REPLACED_QTY"),OOQL.CreateProperty("SelectNode.ISSUE_RECEIPT_QTY")),
                    new SetItem(OOQL.CreateProperty("WAREHOUSE_ID"),OOQL.CreateProperty("SelectNode.WAREHOUSE_ID")),
                    new SetItem(OOQL.CreateProperty("BIN_ID"),OOQL.CreateProperty("SelectNode.BIN_ID")),
                    new SetItem(OOQL.CreateProperty("ITEM_LOT_ID"),OOQL.CreateProperty("SelectNode.ITEM_LOT_ID")),
                    new SetItem(OOQL.CreateProperty("COST_DOMAIN_ID.RTK"),OOQL.CreateProperty("SelectNode.COST_DOMAIN_ID_RTK")),
                    new SetItem(OOQL.CreateProperty("COST_DOMAIN_ID.ROid"),OOQL.CreateProperty("SelectNode.COST_DOMAIN_ID_ROid")),
                })
                .From(selectNode, "SelectNode")
                .Where(OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_D.ISSUE_RECEIPT_D_ID") == OOQL.CreateProperty("SelectNode.ISSUE_RECEIPT_D_ID"));
            #endregion

            qrySrv.ExecuteNoQueryWithManageProperties(updateNode);
        }

        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="tmpIssueReceiptD"></param>
        /// <returns></returns>
        public QueryNode GroupNode(IDataEntityType tmpIssueReceiptD, bool isEntityLine) {
            List<QueryProperty> properties = new List<QueryProperty>{OOQL.CreateProperty("TMP.DOC_NO")
                    , OOQL.CreateProperty("TMP.SequenceNumber")
                    , OOQL.CreateProperty("TMP.PLANT_CODE")
                    , OOQL.CreateProperty("TMP.ITEM_CODE")
                    , OOQL.CreateProperty("TMP.ITEM_FEATURE_CODE")
                    , OOQL.CreateProperty("TMP.UNIT_CODE")
                    , OOQL.CreateProperty("TMP.WAREHOUSE_CODE")
                    , OOQL.CreateProperty("TMP.BIN_CODE")
                    , OOQL.CreateProperty("TMP.LOT_CODE")
            };

            List<QueryProperty> groupProperties = new List<QueryProperty>();
            groupProperties = new List<QueryProperty>();
            groupProperties.AddRange(properties);

            if (!isEntityLine) {
                properties.Add(OOQL.CreateProperty("TMP.BARCODE_NO"));
                groupProperties.Add(OOQL.CreateProperty("TMP.BARCODE_NO"));
            }

            properties.Add(Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("TMP.ISSUE_RECEIPT_QTY")), OOQL.CreateConstants(0), "ISSUE_RECEIPT_QTY"));

            QueryNode node = OOQL.Select(properties
                )
                .From(tmpIssueReceiptD.Name, "TMP")
                .GroupBy(groupProperties);

            return node;
        }

        /// <summary>
        /// 先删除BCLine，后面再重新生成
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpIssueReceiptD"></param>
        private void DeleteBCLine(IQueryService qrySrv, IDataEntityType tmpIssueReceiptD) {
            QueryNode deleteNode = OOQL.Delete("BC_LINE")
                .Where(OOQL.CreateProperty("SOURCE_ID.ROid").In(
                    OOQL.Select(OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_D_ID"))
                    .From("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                    .InnerJoin(tmpIssueReceiptD.Name, "TMP")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO") == OOQL.CreateProperty("TMP.DOC_NO"))
                    .InnerJoin("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                    .On(OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID"))
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
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                Formulas.NewId("BC_LINE_ID"),  //主键
                OOQL.CreateProperty("TmpTable.BARCODE_NO","BARCODE_NO"),  //条码CODE
                OOQL.CreateConstants("ISSUE_RECEIPT.ISSUE_RECEIPT_D","SOURCE_ID.RTK"),  //来源单据类型
                OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_D_ID","SOURCE_ID.ROid"),  //来源单据
                Formulas.Ext("UNIT_CONVERT", "QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("TmpTable.ISSUE_RECEIPT_QTY")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //数量
                Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),  //仓库  //20161208 modi by shenbao fro P001-161208001
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("BIN.BIN_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("TmpTable.BIN_CODE")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BIN_ID")  //库位  //20161208 modi by shenbao fro P001-161208001
            });
            #endregion

            QueryNode groupNode = GroupNode(tmpIssueReceiptD, false);
            QueryNode insertNode = OOQL.Select(
                     properties
                 )
                .From(groupNode, "TmpTable")
                .InnerJoin("ISSUE_RECEIPT")
                .On(OOQL.CreateProperty("TmpTable.DOC_NO") == OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO"))
                .InnerJoin("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                .On(OOQL.CreateProperty("TmpTable.SequenceNumber") == OOQL.CreateProperty("ISSUE_RECEIPT_D.SequenceNumber")
                    & OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("TmpTable.ITEM_CODE") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .InnerJoin("UNIT")
                .On(OOQL.CreateProperty("TmpTable.UNIT_CODE") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("TmpTable.WAREHOUSE_CODE") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                    & OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.ROid") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("TmpTable.BIN_CODE") == OOQL.CreateProperty("BIN.BIN_CODE"))
                .Where(OOQL.CreateProperty("TmpTable.BARCODE_NO") != OOQL.CreateConstants(""));

            //执行插入
            UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "BC_LINE", insertNode, properties.Select(c => c.Alias).ToArray());
        }

        /// <summary>
        /// 获取新插入的领料单，重新保存
        /// </summary>
        /// <param name="docNos">单号集合</param>
        /// <returns></returns>
        private DependencyObjectCollection GetIssueReceipt(List<string> docNos) {
            QueryNode node = OOQL.Select(
                    "ISSUE_RECEIPT.ISSUE_RECEIPT_ID",
                    "DOC.AUTO_APPROVE"
                )
                .From("ISSUE_RECEIPT")
                .InnerJoin("DOC")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT.DOC_ID") == OOQL.CreateProperty("DOC.DOC_ID"))
                .Where(OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO").In(OOQL.CreateDyncParameter("docnos", docNos.ToArray())));
            return this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 创建批量修改所需要的DataTable和Mapping
        /// </summary>
        private void CreateRelateTable(ref DataTable issueReceiptD
            , ref List<BulkCopyColumnMapping> issueReceiptDMap) {
            #region 创建领料单身表
            string[] issueReceiptDColumns = new string[]{
                    "DOC_NO",  //单号
                    "SequenceNumber",  //序号
                    "ISSUE_RECEIPT_QTY",  //领退料数量
                    "PLANT_CODE",  //工厂编号
                    "ITEM_CODE",  //品号
                    "ITEM_FEATURE_CODE",  //特征码
                    "UNIT_CODE",  //单位编号
                    "WAREHOUSE_CODE",  //仓库编号
                    "BIN_CODE" ,  //库位编号
                    "LOT_CODE",  //批号
                    "BARCODE_NO"  //条码
            };
            issueReceiptD = UtilsClass.CreateDataTable("ISSUE_RECEIPT_D", issueReceiptDColumns,
                    new Type[]{
                        typeof(string),  //单号
                        typeof(int),  //序号
                        typeof(decimal),  //领退料数量
                        typeof(string),  //工厂编号
                        typeof(string),  //品号
                        typeof(string),  //特征码
                        typeof(string),  //单位编号
                        typeof(string),  //仓库编号
                        typeof(string),  //库位编号
                        typeof(string),  //批号
                        typeof(string)  //条码
                    });

            //创建map对照表
            Dictionary<string, string> dicIssueReceiptD = new Dictionary<string, string>();
            foreach (string key in issueReceiptDColumns)
                dicIssueReceiptD.Add(key, key);
            issueReceiptDMap = UtilsClass.CreateBulkMapping(dicIssueReceiptD);
            #endregion
        }

        /// <summary>
        /// 存储所需修改IssueReceiptD的数据集合的临时表
        /// </summary>
        private IDataEntityType CreateIssueReceiptDTmpTable(IQueryService qrySrv) {
            string typeName = "Temp_UpdateIssueReceiptD_" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });

            IBusinessTypeService businessSrv = this.GetServiceForThisTypeKey<IBusinessTypeService>();
            SimplePropertyAttribute simplePrimaryAttri = businessSrv.SimplePrimaryKey;
            SimplePropertyAttribute qtyAttri = businessSrv.SimpleQuantity;
            SimplePropertyAttribute tempAttr;

            #region 字段
            //单号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("DOC_NO", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //序号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("SequenceNumber", typeof(int), 0, false, new Attribute[] { tempAttr });
            //拣货数量  //该数量等价于规格中的picking_qty
            defaultType.RegisterSimpleProperty("ISSUE_RECEIPT_QTY", businessSrv.SimpleQuantityType, 0m, false, new Attribute[] { qtyAttri });
            //工厂编号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("PLANT_CODE", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //品号
            defaultType.RegisterSimpleProperty("ITEM_CODE", businessSrv.SimpleItemCodeType, "", false, new Attribute[] { businessSrv.SimpleItemCode });
            //特征码
            defaultType.RegisterSimpleProperty("ITEM_FEATURE_CODE", businessSrv.SimpleItemFeatureType, "", false, new Attribute[] { businessSrv.SimpleItemFeature });
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            //单位编号
            defaultType.RegisterSimpleProperty("UNIT_CODE", typeof(string), "", false, new Attribute[] { tempAttr });
            //仓库编号
            defaultType.RegisterSimpleProperty("WAREHOUSE_CODE", typeof(string), "", false, new Attribute[] { tempAttr });
            //库位编号
            defaultType.RegisterSimpleProperty("BIN_CODE", typeof(string), "", false, new Attribute[] { tempAttr });
            //批号
            defaultType.RegisterSimpleProperty("LOT_CODE", businessSrv.SimpleLotCodeType, "", false, new Attribute[] { businessSrv.SimpleLotCode });
            //条码
            defaultType.RegisterSimpleProperty("BARCODE_NO", typeof(string), "", false, new Attribute[] { tempAttr });
            #endregion

            qrySrv.CreateTempTable(defaultType);

            return defaultType;
        }

        //20170905 add by wangyq for P001-170717001  =================begin===================
        /// <summary>
        /// 利用临时表关联实体表进行批量更新领料单
        /// </summary>
        private void UpdateBcCheckStatus(IQueryService qrySrv, IDataEntityType tempEntityD) {
            QueryNode selectNode = OOQL.Select(true, OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_D_ID"))
                .From("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                .InnerJoin("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID"))
                .InnerJoin(tempEntityD.Name, "TEMP")
                .On(OOQL.CreateProperty("TEMP.DOC_NO") == OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO")
                & OOQL.CreateProperty("TEMP.SequenceNumber") == OOQL.CreateProperty("ISSUE_RECEIPT_D.SequenceNumber"));

            QueryNode node = OOQL.Update("ISSUE_RECEIPT.ISSUE_RECEIPT_D")
                .Set(new SetItem[] { new SetItem(OOQL.CreateProperty("BC_CHECK_STATUS"), OOQL.CreateConstants("2")) })
                .From(selectNode, "selectNode")
                .Where(OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_D.ISSUE_RECEIPT_D_ID") == OOQL.CreateProperty("selectNode.ISSUE_RECEIPT_D_ID"));
            qrySrv.ExecuteNoQueryWithManageProperties(node);
        }
        //20170905 add by wangyq for P001-170717001  =================end===================
        #endregion
    }
}
