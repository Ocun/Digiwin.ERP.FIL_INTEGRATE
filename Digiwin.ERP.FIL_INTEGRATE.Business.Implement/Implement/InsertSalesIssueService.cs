//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/10 10:19:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>产生销货出库单服务实现</description>
//----------------------------------------------------------------  
//20161208 modi by shenbao fro P001-161208001
//20170209 modi by liwei1 for P001-170203001 修正单据类型取值
//20170310 modi by shenbao for B001-170309014 null值处理
//20170330 modi by wangrm for P001-170328001
//20170619 modi by zhangcn for P001-170606002

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
using Digiwin.ERP.EFNET.Business;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [SingleGetCreator]
    [ServiceClass(typeof(IInsertSalesIssueService))]
    [Description("产生销货出库单服务实现")]
    public sealed class InsertSalesIssueService : ServiceComponent, IInsertSalesIssueService {
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

        private IPrimaryKeyService _primaryKeySrv;
        /// <summary>
        /// 主键生成服务
        /// </summary>
        public IPrimaryKeyService PrimaryKeySrv {
            get {
                if (_primaryKeySrv == null)
                    _primaryKeySrv = this.GetService<IPrimaryKeyService>("SALES_ISSUE");

                return _primaryKeySrv;
            }
        }

        private IDocumentNumberGenerateService _documentNumberGenSrv;
        /// <summary>
        /// 生成单号服务
        /// </summary>
        public IDocumentNumberGenerateService DocumentNumberGenSrv {
            get {
                if (_documentNumberGenSrv == null)
                    _documentNumberGenSrv = this.GetService<IDocumentNumberGenerateService>("SALES_ISSUE");

                return _documentNumberGenSrv;
            }
        }

        #endregion

        #region 自定义字段

        /// <summary>
        /// 工厂对应的单据类型信息
        /// </summary>
        private DependencyObjectCollection _docInfos;

        /// <summary>
        /// 记录每个单据类型对应的当前单号
        /// </summary>
        private Hashtable _currentDocNo = new Hashtable();

        #endregion

        #region IInsertSalesIssueService 成员

        /// <summary>
        ///  产生销货出库单
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型 1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collection">接口传入的领料单单身数据集合</param>
        public DependencyObjectCollection InsertSalesIssue(string employeeNo, string scanType, DateTime reportDatetime, string pickingDepartmentNo
            , string recommendedOperations, string recommendedFunction, string scanDocNo, DependencyObjectCollection collection) {
            DependencyObjectCollection rtnColl = CreateReturnCollection();
            #region 参数检查
            if (Maths.IsEmpty(recommendedOperations)) {  //20161208 add by shenbao fro P001-161208001
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "recommended_operations" }));
            }
            if (Maths.IsEmpty(recommendedFunction)) {  //20161208 add by shenbao fro P001-161208001
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "recommended_function" }));
            }
            #endregion

            //创建临时表需要的DataTable和Mapping信息
            DataTable salesIssue = null, salesIssueD = null;
            List<BulkCopyColumnMapping> salesIssueMap = null, salesIssueDMap = null;
            this.CreateRelateTable(ref salesIssue, ref salesIssueD,
                ref salesIssueMap, ref salesIssueDMap);

            List<DocumentInfo> listDocuments = new List<DocumentInfo>();  //记录生成的单据信息

            //组织数据BulkCopy需要的DataTable数据
            GetDocInfo(recommendedOperations, collection);
            if (_docInfos.Count <= 0)
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111275"));
            InsertDataTable(salesIssue, salesIssueD, collection, listDocuments, employeeNo, pickingDepartmentNo);
            if (salesIssue.Rows.Count <= 00 || salesIssueD.Rows.Count <= 0)  //没有数据值不再往下执行
                return rtnColl;

            #region 新增逻辑
            using (ITransactionService trans = this.GetService<ITransactionService>()) {
                IQueryService querySrv = this.GetService<IQueryService>();

                //新增临时表
                IDataEntityType salesIssueTmp = CreateSalesIssueTmpTable(querySrv);
                IDataEntityType salesIssueDTmp = CreateSalesIssueDTmpTable(querySrv);

                //批量新增到临时表
                querySrv.BulkCopy(salesIssue, salesIssueTmp.Name, salesIssueMap.ToArray());
                querySrv.BulkCopy(salesIssueD, salesIssueDTmp.Name, salesIssueDMap.ToArray());

                //利用临时表批量新增相关数据
                InsertSalesIssue(querySrv, salesIssueTmp, reportDatetime);
                InsertSalesIssueD(querySrv, salesIssueDTmp);
                InsertBCLine(querySrv, salesIssueDTmp);

                //修改单头汇总字段
                UpdateSalesIssue(querySrv, listDocuments.Select(c => c.ID).ToArray());
                
                //EFNET签核
                var infos = listDocuments.GroupBy(c => new { c.DOC_ID, c.OwnerOrgID });
                foreach (var item in infos) {
                    IEFNETStatusStatusService efnetSrv = this.GetService<IEFNETStatusStatusService>();
                    efnetSrv.GetFormFlow("SALES_ISSUE.I01", item.Key.DOC_ID, item.Key.OwnerOrgID,
                         item.Select(c => c.ID).ToArray());
                }

                //保存单据
                IReadService readSrv = GetService<IReadService>("SALES_ISSUE");
                object[] entities = readSrv.Read(listDocuments.Select(c => c.ID).Distinct().ToArray());
                if (entities != null && entities.Length > 0) {
                    ISaveService saveSrv = this.GetService<ISaveService>("SALES_ISSUE");
                    saveSrv.Save(entities);
                }

                trans.Complete();
            }
            #endregion

            #region 组织返回结果

            foreach (DocumentInfo item in listDocuments) {
                DependencyObject obj = rtnColl.AddNew();
                obj["doc_no"] = item.DOC_NO;
            }

            #endregion

            #region 释放内存
            ClearData();
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
        /// 获取每笔记录对应的单据类型
        /// 用于后面生成单号使用
        /// 后面以工厂来取得，因为一个工厂对于相同的Catagory在FIL单据类型设置只可能有一个单据类型
        /// </summary>
        /// <param name="recommendedOperations"></param>
        /// <param name="collection"></param>
        /// <returns></returns>
        private void GetDocInfo(string recommendedOperations, DependencyObjectCollection collection){
            if (collection.Count == 0){return;}//20170209 add by liwei1 for P001-170203001
            QueryNode node = OOQL.Select(true
                    , "PLANT.PLANT_CODE"
                    , "PLANT.PLANT_ID"
                    , "PLANT.COMPANY_ID"
                    , "PARA_DOC_FIL.DOC_ID"
                    , "DOC.SEQUENCE_DIGIT"  //流水号位数
                    , "PARA_DOC_FIL.SOURCE_DOC_ID"//20170209 add by liwei1 for P001-170203001 
                )
                .From("PLANT", "PLANT")
                .InnerJoin("PARA_DOC_FIL")
                .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid"))
                .InnerJoin("DOC")
                .On(OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID") == OOQL.CreateProperty("DOC.DOC_ID")
                    & OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants("14"))
                .Where(OOQL.AuthFilter("PLANT", "PLANT")
                    //& OOQL.CreateProperty("PLANT.PLANT_CODE").In(OOQL.CreateDyncParameter("CODES", collection.Select(c => c["site_no"].ToStringExtension()).ToArray())));20170209 mark by liwei1 for P001-170203001
                    //20170209 add by liwei1 for P001-170203001 ===begin===
                       & ((OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(collection[0]["site_no"]))
                          & ((OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                             | (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.Select(1, OOQL.CreateProperty("DOC_ID"))
                                 .From("SALES_DELIVERY")
                                 .Where((OOQL.CreateProperty("DOC_NO") ==OOQL.CreateConstants(collection[0]["info_lot_no"])))))))
                .OrderBy(
                    OOQL.CreateOrderByItem(
                        OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"), SortType.Desc));
            //20170209 add by liwei1 for P001-170203001 ===end===

            _docInfos = this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 组织更新salesIssue salesIssueD DataTable
        /// </summary>
        /// <param name="salesIssue"></param>
        private void InsertDataTable(DataTable salesIssue, DataTable salesIssueD
            , DependencyObjectCollection colls, List<DocumentInfo> documents, string employeeNo, string pickingDepartmentNo) {
            foreach (DependencyObject obj in colls) {
                #region 新增表salesIssue结构
                object key = PrimaryKeySrv.CreateId();  //主键
                string docNo = string.Empty;
                DependencyObject infoItem = _docInfos.Where(c => c["PLANT_CODE"].ToStringExtension() == obj["site_no"].ToStringExtension()).FirstOrDefault();
                if (infoItem == null)
                    break;

                DocumentInfo doc = new DocumentInfo();
                //计算单号
                if (!_currentDocNo.ContainsKey(infoItem["DOC_ID"])) {
                    docNo = UtilsClass.NextNumber(DocumentNumberGenSrv, "", infoItem["DOC_ID"], infoItem["SEQUENCE_DIGIT"].ToInt32(), DateTime.Now);
                    _currentDocNo.Add(infoItem["DOC_ID"], docNo);
                } else {
                    docNo = UtilsClass.NextNumber(DocumentNumberGenSrv, _currentDocNo[infoItem["DOC_ID"]].ToStringExtension(), infoItem["DOC_ID"], infoItem["SEQUENCE_DIGIT"].ToInt32(), DateTime.Now);
                    _currentDocNo[infoItem["DOC_ID"]] = docNo;
                }

                //组织表结构
                DataRow dr = salesIssue.NewRow();
                dr["ID"] = key;  //主键
                dr["doc_id"] = infoItem["DOC_ID"];  //单据类型
                dr["doc_no"] = docNo;  //单号
                dr["employee_no"] = employeeNo;  //人员
                dr["picking_department_no"] = pickingDepartmentNo;  //部门
                dr["site_no"] = obj["site_no"]; //工厂编号
                dr["info_lot_no"] = obj["info_lot_no"];  //信息批号
                dr["plant_id"] = infoItem["PLANT_ID"];  //工厂ID
                dr["company_id"] = infoItem["COMPANY_ID"];  //工厂对应的公司ID

                //20170619 add by zhangcn for P001-170606002 ===beigin===
                DependencyObjectCollection collSalesSyneryFiD =  QuerySalesSyneryFiD(obj["info_lot_no"]);
                if (collSalesSyneryFiD.Count > 0){
                    dr["DOC_Sequence"] = collSalesSyneryFiD[0]["SequenceNumber"].ToInt32();
                    dr["GROUP_SYNERGY_D_ID"] = collSalesSyneryFiD[0]["SALES_SYNERGY_FI_D_ID"];
                }
                else{
                    dr["DOC_Sequence"] = 0;
                    dr["GROUP_SYNERGY_D_ID"] = Maths.GuidDefaultValue();
                }
                //20170619 add by zhangcn for P001-170606002 ===end===
 
                //记录单据信息
                doc.ID = key;
                doc.DOC_ID = infoItem["DOC_ID"];
                doc.OwnerOrgID = infoItem["PLANT_ID"];
                doc.DOC_NO = docNo;
                documents.Add(doc);

                salesIssue.Rows.Add(dr);

                #endregion

                DependencyObjectCollection subColls = obj["scan_detail"] as DependencyObjectCollection;
                if (subColls != null && subColls.Count > 0) {
                    //序号
                    int sequenceno = 1;
                    //根据唯一性字段来记录对应的行信息
                    Dictionary<string, EntityLine> lineKeyDic = new Dictionary<string, EntityLine>(); 
                    foreach (DependencyObject subObj in subColls) {
                        //唯一性字段组合：信息批号+源单单号+序号+品号+特征码+仓库+库位+批号+单位+工厂
                        string uniqueKey = string.Concat(subObj["info_lot_no"].ToStringExtension(), subObj["doc_no"].ToStringExtension(), subObj["seq"].ToStringExtension()
                        , subObj["item_no"].ToStringExtension(), subObj["item_feature_no"].ToStringExtension(), subObj["warehouse_no"].ToStringExtension()
                        , subObj["storage_spaces_no"].ToStringExtension(), subObj["lot_no"].ToStringExtension(), subObj["picking_unit_no"].ToStringExtension()
                        , subObj["site_no"].ToStringExtension());
                        #region 新增表salesIssueD结构

                        //组织DataTable
                        dr = salesIssueD.NewRow();
                        dr["SALES_ISSUE_ID"] = key;  //父主键
                        if (!lineKeyDic.ContainsKey(uniqueKey)) {  //新的一组，重新生成行主键和行号
                            EntityLine line = new EntityLine();
                            line.UniqueKey = uniqueKey;
                            line.Key = PrimaryKeySrv.CreateId();
                            line.SequenceNumber = sequenceno++;

                            dr["SALES_ISSUE_D_ID"] = line.Key;
                            dr["SequenceNumber"] = line.SequenceNumber;

                            lineKeyDic.Add(uniqueKey, line);
                        } else {  //已经存在的
                            dr["SALES_ISSUE_D_ID"] = lineKeyDic[uniqueKey].Key;
                            dr["SequenceNumber"] = lineKeyDic[uniqueKey].SequenceNumber;
                        }
                        dr["info_lot_no"] = subObj["info_lot_no"];  //信息批号
                        dr["item_no"] = subObj["item_no"];  //品号
                        dr["item_feature_no"] = subObj["item_feature_no"];  //特征码
                        dr["picking_unit_no"] = subObj["picking_unit_no"];  //单位
                        dr["doc_no"] = subObj["doc_no"];  //单号
                        dr["seq"] = subObj["seq"].ToInt32();  //来源序号
                        dr["warehouse_no"] = subObj["warehouse_no"];  //仓库编号
                        dr["storage_spaces_no"] = subObj["storage_spaces_no"];  //库位编号
                        dr["lot_no"] = subObj["lot_no"];  //批号
                        dr["picking_qty"] = subObj["picking_qty"].ToDecimal();  //拣货数量
                        dr["barcode_no"] = subObj["barcode_no"];  //条码编号
                        dr["site_no"] = subObj["site_no"];  //工厂编码
                        dr["doc_id"] = infoItem["DOC_ID"];  //单据类型

                        salesIssueD.Rows.Add(dr);
                        #endregion
                    }
                }
            }
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增销货出库单
        /// </summary>
        private void InsertSalesIssue(IQueryService qrySrv, IDataEntityType tmpSalesIssue, DateTime reportDatetime) {
            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                OOQL.CreateProperty("tmpTable.ID","SALES_ISSUE_ID"),  //主键
                OOQL.CreateProperty("tmpTable.doc_id","DOC_ID"),  //单据类型
                OOQL.CreateConstants(reportDatetime,"DOC_DATE"),  //单据日期
                OOQL.CreateProperty("tmpTable.doc_no","DOC_NO"),  //单号
                OOQL.CreateConstants("14","CATEGORY"),  //单据性质码
                OOQL.CreateConstants(reportDatetime,"TRANSACTION_DATE"),//交易日期
                OOQL.CreateConstants("PLANT", "Owner_Org.RTK"),  //组织
                Formulas.IsNull(OOQL.CreateProperty("tmpTable.plant_id"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org.ROid"),  //组织ID  //20170310 modi by shenbao for B001-170309014
                OOQL.CreateConstants(-1,"STOCK_ACTION"),//库存影响
                OOQL.CreateConstants(0,"PIECES"),//件数
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"WAREHOUSE_ID"),//限用仓库编号
                OOQL.CreateProperty("tmpTable.company_id","COMPANY_ID"),//工厂对应的公司
                Formulas.IsNull(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Dept"),  //部门  //20170310 modi by shenbao for B001-170309014
                Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Emp"),  //人员  //20170310 modi by shenbao for B001-170309014
                OOQL.CreateConstants("","ACCOUNT_YEAR"),  //会计年度
                OOQL.CreateConstants(0,"ACCOUNT_PERIOD_SEQNO"),//会计期间序号
                OOQL.CreateConstants("","ACCOUNT_PERIOD_CODE"),//会计期间期号
                OOQL.CreateProperty("SALES_DELIVERY.CUSTOMER_ID","SHIP_TO_CUSTOMER_ID"),//收货客户
                OOQL.CreateConstants(false,"SOURCE_UNCONFIRM"),  //源单撤审
                OOQL.CreateProperty("tmpTable.doc_no","VIEW_DOC_NO"),//显示单号
                OOQL.CreateConstants(0,"SUM_BUSINESS_QTY"),//业务数量合计
                Formulas.Case(null,OOQL.CreateConstants(false),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY.SIGN_REQUIRED")==OOQL.CreateConstants(true)
                        ,OOQL.CreateConstants(true))
                },"SIGN_REQUIRED"),//需签收
                //20170619 add by zhangcn for P001-170606002 ===beigin===
                Formulas.IsNull(OOQL.CreateProperty("SALES_DELIVERY.ALL_SYNERGY"),OOQL.CreateConstants(false), "ALL_SYNERGY"),
                OOQL.CreateProperty("tmpTable.DOC_Sequence", "DOC_Sequence"),
                OOQL.CreateProperty("tmpTable.GROUP_SYNERGY_D_ID", "GROUP_SYNERGY_D_ID"),
                OOQL.CreateConstants(string.Empty,"GENERATE_NO"),
                OOQL.CreateConstants(false,"GENERATE_STATUS"),
                Formulas.IsNull(OOQL.CreateProperty("SALES_DELIVERY.GROUP_SYNERGY_ID.ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "GROUP_SYNERGY_ID_ROid"),
                Formulas.IsNull(OOQL.CreateProperty("SALES_DELIVERY.GROUP_SYNERGY_ID.RTK"),OOQL.CreateConstants(string.Empty), "GROUP_SYNERGY_ID_RTK"),
                Formulas.IsNull(OOQL.CreateProperty("SALES_DELIVERY.SOURCE_CUSTOMER_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_CUSTOMER_ID"),
                //20170619 add by zhangcn for P001-170606002 ===end===
            });
               
                
            #endregion

            QueryNode insertNode = OOQL.Select(
                properties
                )
                .From(tmpSalesIssue.Name, "tmpTable")
                .LeftJoin("EMPLOYEE")
                .On(OOQL.CreateProperty("tmpTable.employee_no") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE"))
                .LeftJoin("ADMIN_UNIT")
                .On(OOQL.CreateProperty("tmpTable.picking_department_no") ==
                    OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE"))
                .LeftJoin("SALES_DELIVERY")
                .On(OOQL.CreateProperty("tmpTable.info_lot_no") == OOQL.CreateProperty("SALES_DELIVERY.DOC_NO"));

            //执行插入
            UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "SALES_ISSUE", insertNode, properties.Select(c => c.Alias).ToArray());
        }

        private void UpdateSalesIssue(IQueryService qrySrv, object[] ids) {
            if (ids == null || ids.Length <= 0)
                return;
            List<QueryProperty> idList = new List<QueryProperty>();
            ids.ToList().ForEach(c => idList.Add(OOQL.CreateConstants(c)));

            QueryNode node = OOQL.Select(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID")
                    , Formulas.Sum(OOQL.CreateProperty("SALES_ISSUE_D.BUSINESS_QTY"), "BUSINESS_QTY")
                )
                .From("SALES_ISSUE")
                .InnerJoin("SALES_ISSUE.SALES_ISSUE_D", "SALES_ISSUE_D")
                .On(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID") == OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_ID"))
                .Where(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID").In(idList))
                .GroupBy(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID"));

            QueryNode update = OOQL.Update("SALES_ISSUE")
                .Set(new SetItem[]{
                    new SetItem(OOQL.CreateProperty("SUM_BUSINESS_QTY"),OOQL.CreateProperty("SubNode.BUSINESS_QTY"))
                })
                .From(node, "SubNode")
                .Where(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID") == OOQL.CreateProperty("SubNode.SALES_ISSUE_ID"));

            UtilsClass.ExecuteNoQuery(qrySrv, update, false);
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增销货出库单身
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpBCLine"></param>
        private int InsertSalesIssueD(IQueryService qrySrv, IDataEntityType tmpSalesIssueD) {
            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                OOQL.CreateProperty("tmpTable.SequenceNumber","SequenceNumber"),  //序号
                OOQL.CreateProperty("tmpTable.SALES_ISSUE_D_ID","SALES_ISSUE_D_ID"),  //主键
                OOQL.CreateProperty("tmpTable.SALES_ISSUE_ID","SALES_ISSUE_ID"),  //父主键
                OOQL.CreateProperty("ITEM.ITEM_ID","ITEM_ID"),  //品号
                OOQL.CreateProperty("ITEM.ITEM_NAME","ITEM_DESCRIPTION"),  //品名
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.item_feature_no")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"ITEM_FEATURE_ID"),  //特征码
                OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_SPECIFICATION","ITEM_SPECIFICATION"),  //规格
                Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),  //仓库
                OOQL.CreateProperty("tmpTable.picking_qty", "BUSINESS_QTY"),  //业务数量
                Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "BUSINESS_UNIT_ID"),  //业务单位
                OOQL.CreateConstants(0, "PRICE_QTY"),  //计价数量
                Formulas.IsNull(OOQL.CreateProperty("SALES_DELIVERY_D.PACKING_MODE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"PACKING_MODE_ID"),//包装方式  //20170310 modi by shenbao for B001-170309014
                Formulas.Ext("UNIT_CONVERT", "SECOND_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.SECOND_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //第二数量
                Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //库存数量
                OOQL.CreateConstants("", "REMARK"),  //备注
                OOQL.CreateConstants(0, "PIECES"),  //件数
                OOQL.CreateConstants("SALES_DELIVERY.SALES_DELIVERY_D", "SOURCE_ID.RTK"),  //源单RTK
                Formulas.IsNull(OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_D_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_ID.ROid"),  //源单ROid
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("BIN.BIN_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.storage_spaces_no")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BIN_ID"),  //库位
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.lot_no")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"ITEM_LOT_ID"),  //批号
                Formulas.Case(null, OOQL.CreateConstants("COST_DOMAIN"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(1),
                        OOQL.CreateConstants("COMPANY"))
                }, "COST_DOMAIN_ID_RTK"),  //成本域类型
                Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants(Maths.GuidDefaultValue()), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(1),
                        OOQL.CreateProperty("PLANT.PLANT_ID")),
                    new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(2),
                        OOQL.CreateProperty("PLANT.COST_DOMAIN_ID")),
                    new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(3),
                        OOQL.CreateProperty("WAREHOUSE.COST_DOMAIN_ID"))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()), "COST_DOMAIN_ID_ROid"),  //成本域
                Formulas.Case(null,OOQL.CreateConstants("0"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY.SIGN_REQUIRED")==OOQL.CreateConstants(true),
                        OOQL.CreateConstants("1"))
                },"SIGN_TYPE"),//签收状态
                OOQL.CreateConstants("1","SETTLEMENT_CLOSE"),//结算状态
                Formulas.Case(null,OOQL.CreateConstants("0"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID").IsNotNull()
                        & OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID")!=OOQL.CreateConstants(Maths.GuidDefaultValue())
                        & OOQL.CreateProperty("SALES_DELIVERY.SIGN_REQUIRED")==OOQL.CreateConstants(false)
                            ,OOQL.CreateConstants("1"))
                },"INNER_SETTLEMENT_CLOSE"),//内部结算码
                OOQL.CreateConstants(0,"SHOULD_SETTLE_COST_ACC"),  //应结算成本占比
                OOQL.CreateConstants(0,"SHOULD_SETTLE_PRICE_QTY"),  //应结算计价数量
                Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants(Maths.GuidDefaultValue()), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID").IsNotNull()
                        & OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID")!=OOQL.CreateConstants(Maths.GuidDefaultValue())
                            ,OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID")),
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD")
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.ROid").IsNotNull()
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.ROid")!=OOQL.CreateConstants(Maths.GuidDefaultValue())
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_SYNERGY"),
                        OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.ROid"))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SYNERGY_ID"),  //协同关系ID
                Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants(Maths.GuidDefaultValue()), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID").IsNotNull()
                        & OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID")!=OOQL.CreateConstants(Maths.GuidDefaultValue())
                            ,OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_D_ID")),
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD")
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.ROid").IsNotNull()
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.ROid")!=OOQL.CreateConstants(Maths.GuidDefaultValue())
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_SYNERGY"),
                        OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_FI_D_ID"))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SYNERGY_D_ID"),  //协同序号ID
                OOQL.CreateConstants("OTHER","SYNERGY_SOURCE_ID.RTK"),//协同源单单身ID.RTK
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"SYNERGY_SOURCE_ID.ROid"),//协同源单单身ID.ROid
                Formulas.Case(null, OOQL.CreateConstants(""), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD")
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.ROid").IsNotNull()
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.ROid")!=OOQL.CreateConstants(Maths.GuidDefaultValue())
                        & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_SYNERGY"),
                        OOQL.CreateConstants("1")),
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID").IsNotNull()
                        & OOQL.CreateProperty("SALES_DELIVERY.SYNERGY_ID")!=OOQL.CreateConstants(Maths.GuidDefaultValue())
                            ,OOQL.CreateConstants("0"))
                }, "SYNERGY_TYPE"),  //协同关系类型
                OOQL.CreateConstants(OrmDataOption.EmptyDateTime,GeneralDBType.Date,"PLAN_SETTLEMENT_DATE"),//预计结算日期
                OOQL.CreateConstants("N","VMI_SETTLED"),//VMI结算码
                Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("DOC.BC_CHECK")==OOQL.CreateConstants("0"),
                        OOQL.CreateConstants("0"))
                }, "BC_CHECK_STATUS"),  //检核码
                Formulas.Case(null, OOQL.CreateConstants("OTHER"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD")
                            ,OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD")),
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK")==OOQL.CreateConstants("INNER_ORDER_DOC.INNER_ORDER_DOC_D.INNER_ORDER_DOC_SD")
                            ,OOQL.CreateConstants("INNER_ORDER_DOC.INNER_ORDER_DOC_D.INNER_ORDER_DOC_SD")),
                }, "ORDER_SOURCE_ID.RTK"),  //来源订单RTK
                Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants(Maths.GuidDefaultValue()), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD")
                        |OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK")==OOQL.CreateConstants("INNER_ORDER_DOC.INNER_ORDER_DOC_D.INNER_ORDER_DOC_SD")
                            ,OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.ROid"))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()), "ORDER_SOURCE_ID.ROid"),  //来源订单ROid
                OOQL.CreateConstants(0,"SETTLEMENT_PATH_TYPE"),//结算路径类型
                OOQL.CreateConstants(0,"RE_SETTLEMENT_PATH_TYPE"),//退货结算路径类型
                OOQL.CreateConstants(true,"SETTLEMENT_START_INDICATOR"),//结算起点
                OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_TYPE","ITEM_TYPE"),//商品类型
                Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT")==OOQL.CreateConstants(false)
                        |OOQL.CreateProperty("tmpTable.picking_qty")==OOQL.CreateConstants(0,GeneralDBType.Decimal)
                        |(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.RTK")==OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD")
                            &OOQL.CreateProperty("SALES_ORDER_DOC_SD.DIRECT_SHIP")==OOQL.CreateConstants(true)
                            &OOQL.CreateProperty("SALES_ORDER_DOC_SD.DELIVERY_TYPE")==OOQL.CreateConstants("3")),
                        OOQL.CreateConstants("0"))
                }, "SN_COLLECTED_STATUS"),  //序列号检核码
                //20170619 add by zhangcn for P001-170606002 ===begin===
                Formulas.IsNull(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ORDER.ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue(),GeneralDBType.Guid), "SOURCE_ORDER_ROid"),
                Formulas.IsNull(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ORDER.RTK"),OOQL.CreateConstants(string.Empty), "SOURCE_ORDER_RTK"),
                OOQL.CreateConstants(1,GeneralDBType.Int32,"SYNERGY_SETTLEMENT_GROUP")
               //20170619 add by zhangcn for P001-170606002 ===end===
            });
            #endregion

            QueryNode groupNode = GroupNode(tmpSalesIssueD, true);
            QueryNode insertNode = OOQL.Select(
                     properties
                 )
                .From(groupNode, "tmpTable")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("tmpTable.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("tmpTable.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID")
                    & OOQL.CreateProperty("tmpTable.item_feature_no") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"))
                .InnerJoin("UNIT")
                .On(OOQL.CreateProperty("tmpTable.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("tmpTable.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                    & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("tmpTable.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE"))
                .LeftJoin("SALES_DELIVERY")
                .On(OOQL.CreateProperty("tmpTable.doc_no") == OOQL.CreateProperty("SALES_DELIVERY.DOC_NO"))
                .LeftJoin("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D")
                .On(OOQL.CreateProperty("tmpTable.seq") == OOQL.CreateProperty("SALES_DELIVERY_D.SequenceNumber")
                    & OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_ID") == OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_ID"))
                .LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD")
                .On(OOQL.CreateProperty("SALES_DELIVERY_D.SOURCE_ID.ROid") == OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_SD_ID"))
                .LeftJoin("ITEM_LOT")
                .On(OOQL.CreateProperty("tmpTable.lot_no") == OOQL.CreateProperty("ITEM_LOT.LOT_CODE")
                    & OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_ID")
                    & ((OOQL.CreateProperty("tmpTable.item_feature_no") == OOQL.CreateConstants("")
                        & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                        | (OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID"))))
                .InnerJoin("PARA_COMPANY")
                .On(OOQL.CreateProperty("PLANT.COMPANY_ID") == OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid"))
                .InnerJoin("DOC")
                .On(OOQL.CreateProperty("tmpTable.doc_id") == OOQL.CreateProperty("DOC.DOC_ID"))
                .LeftJoin(
                    OOQL.Select(OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_ID")
                        , OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_FI_D_ID")
                        , Formulas.RowNumber("SEQ", OOQL.Over(new QueryProperty[] { OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_ID") }
                            , new OrderByItem[]{
                                new OrderByItem("SALES_SYNERGY_FI_D.SequenceNumber",SortType.Desc)
                            }))).From("SALES_SYNERGY.SALES_SYNERGY_FI_D", "SALES_SYNERGY_FI_D"), "SALES_SYNERGY_FI_D")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_SD.SYNERGY_ID") == OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_ID")
                    & OOQL.CreateProperty("SALES_SYNERGY_FI_D.SEQ") == OOQL.CreateConstants(1));

            //执行插入
           return UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "SALES_ISSUE.SALES_ISSUE_D", insertNode, properties.Select(c => c.Alias).ToArray());
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增条码交易明细
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpBCLine"></param>
        private void InsertBCLine(IQueryService qrySrv, IDataEntityType tmpSalesIssueD) {
            bool bcLintFlag = UtilsClass.IsBCLineManagement(qrySrv);
            if (!bcLintFlag)
                return;

            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                Formulas.NewId("BC_LINE_ID"),  //主键
                OOQL.CreateProperty("tmpTable.barcode_no","BARCODE_NO"),  //条码CODE
                OOQL.CreateConstants("SALES_ISSUE.SALES_ISSUE_D","SOURCE_ID.RTK"),  //来源单据类型
                OOQL.CreateProperty("tmpTable.SALES_ISSUE_D_ID","SOURCE_ID.ROid"),  //来源单据
                Formulas.Ext("UNIT_CONVERT", "QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //数量
                Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),  //仓库
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("BIN.BIN_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.storage_spaces_no")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BIN_ID"),  //库位
                //20170330 add by wangrm for P001-170328001=====start=======
                Formulas.IsNull(OOQL.CreateProperty("SALES_ISSUE.Owner_Org.RTK"),OOQL.CreateConstants(string.Empty), "Owner_Org.RTK"),
                Formulas.IsNull(OOQL.CreateProperty("SALES_ISSUE.Owner_Org.ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"Owner_Org.ROid"), 
                Formulas.IsNull(OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_DOC_ID"), 
                Formulas.IsNull(OOQL.CreateProperty("SALES_ISSUE.DOC_DATE"),OOQL.CreateConstants(OrmDataOption.EmptyDateTime), "DOC_DATE")
                //20170330 add by wangrm for P001-170328001=====end=======
            });
            #endregion

            QueryNode groupNode = GroupNode(tmpSalesIssueD, false);
            QueryNode insertNode = OOQL.Select(
                     properties
                 )
                .From(groupNode, "tmpTable")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("tmpTable.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("tmpTable.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .InnerJoin("UNIT")
                .On(OOQL.CreateProperty("tmpTable.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("tmpTable.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                    & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("tmpTable.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE"))
                //20170330 add by wangrm for P001-170328001=====start=======
                .LeftJoin("SALES_ISSUE", "SALES_ISSUE")
                .On(OOQL.CreateProperty("tmpTable.SALES_ISSUE_ID") == OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID"))
                //20170330 add by wangrm for P001-170328001=====end=======
                    ;

            //执行插入
            UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "BC_LINE", insertNode, properties.Select(c => c.Alias).ToArray());
        }

        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="tmpIssueReceiptD"></param>
        /// <returns></returns>
        public QueryNode GroupNode(IDataEntityType tmpIssueReceiptD, bool isEntityLine) {
            List<QueryProperty> properties = new List<QueryProperty>{OOQL.CreateProperty("TMP.SALES_ISSUE_D_ID")
                    , OOQL.CreateProperty("TMP.SALES_ISSUE_ID")
                    , OOQL.CreateProperty("TMP.info_lot_no")
                    , OOQL.CreateProperty("TMP.SequenceNumber")
                    , OOQL.CreateProperty("TMP.item_no")
                    , OOQL.CreateProperty("TMP.item_feature_no")
                    , OOQL.CreateProperty("TMP.picking_unit_no")
                    , OOQL.CreateProperty("TMP.doc_no")
                    , OOQL.CreateProperty("TMP.seq")
                    , OOQL.CreateProperty("TMP.warehouse_no")
                    , OOQL.CreateProperty("TMP.storage_spaces_no")
                    , OOQL.CreateProperty("TMP.lot_no")
                    , OOQL.CreateProperty("TMP.site_no")
                    , OOQL.CreateProperty("TMP.doc_id")
            };

            List<QueryProperty> groupProperties = new List<QueryProperty>();
            groupProperties = new List<QueryProperty>();
            groupProperties.AddRange(properties);

            if (!isEntityLine) {
                properties.Add(OOQL.CreateProperty("TMP.barcode_no"));
                groupProperties.Add(OOQL.CreateProperty("TMP.barcode_no"));
            }

            properties.Add(Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("TMP.picking_qty")), OOQL.CreateConstants(0), "picking_qty"));

            QueryNode node = OOQL.Select(properties
                )
                .From(tmpIssueReceiptD.Name, "TMP")
                .GroupBy(groupProperties);

            return node;
        }

        /// <summary>
        /// 释放内存
        /// </summary>
        private void ClearData() {
            _currentDocNo = null;
            _docInfos = null;
            _documentNumberGenSrv = null;
            _encodeSrv = null;
            _primaryKeySrv = null;
        }

        /// <summary>
        /// 创建批量修改所需要的DataTable和Mapping
        /// </summary>
        private void CreateRelateTable(ref DataTable salesIssue, ref DataTable salesIssueD
            , ref List<BulkCopyColumnMapping> salesIssueMap, ref List<BulkCopyColumnMapping> salesIssueDMap) {
            #region 创建销货出库单表
            string[] salesIssueColumns = new string[]{
                    "ID",  //主键
                    "doc_id",  //单据类型
                    "doc_no",  //单号
                    "plant_id",  //工厂id
                    "company_id",  //工厂对应的公司
                    "employee_no",  //人员
                    "picking_department_no",  //部门
                    "site_no",  //工厂编号
                    "info_lot_no",        //信息批号
                    "DOC_Sequence",       //单据顺序 计算字段  //20170619 add by zhangcn for P001-170606002 
                    "GROUP_SYNERGY_D_ID"  //协同序号 计算字段  //20170619 add by zhangcn for P001-170606002      
            };
            salesIssue = UtilsClass.CreateDataTable("SALES_ISSUE", salesIssueColumns,
                    new Type[]{
                        typeof(object),  //主键
                        typeof(object),  //单据类型
                        typeof(string),  //单号
                        typeof(object),  //工厂ID
                        typeof(object),  //工厂对应公司ID
                        typeof(string),  //人员
                        typeof(string),  //部门
                        typeof(string),  //工厂编号
                        typeof(string), //信息批号
                        typeof(int),    //单据顺序 计算字段 //20170619 add by zhangcn for P001-170606002 
                        typeof(object), //协同序号 计算字段 //20170619 add by zhangcn for P001-170606002 
                    });

            //创建map对照表
            Dictionary<string, string> dicSalesIssue = new Dictionary<string, string>();
            foreach (string key in salesIssueColumns)
                dicSalesIssue.Add(key, key);
            salesIssueMap = UtilsClass.CreateBulkMapping(dicSalesIssue);
            #endregion

            #region 创建销货出库单身表
            string[] salesIssueDColumns = new string[]{
                    "SALES_ISSUE_D_ID",  //主键
                    "SALES_ISSUE_ID",  //父主键
                    "info_lot_no",  //信息批号
                    "SequenceNumber",  //序号
                    "item_no",  //品号
                    "item_feature_no",   //特征码
                    "picking_unit_no",  //单位
                    "doc_no",  //单号
                    "seq",  //来源序号
                    "warehouse_no",  //仓库
                    "storage_spaces_no",   //库位
                    "lot_no",  //批号
                    "picking_qty",  //拣货数量
                    "barcode_no",  //条码编号
                    "site_no",  //工厂编码
                    "doc_id"  //单据类型
            };
            salesIssueD = UtilsClass.CreateDataTable("SALES_ISSUE_D", salesIssueDColumns,
                    new Type[]{
                        typeof(object),  //主键
                        typeof(object),  //父主键
                        typeof(string),  //信息批号
                        typeof(int),  //序号
                        typeof(string),  //品号
                        typeof(string),   //特征码
                        typeof(string),  //单位
                        typeof(string),   //单号
                        typeof(string),  //来源序号
                        typeof(string),   //仓库
                        typeof(string),  //库位
                        typeof(string),   //批号
                        typeof(decimal),  //拣货数量
                        typeof(string),   //条码编号
                        typeof(string),   //工厂编码
                        typeof(object)  //单据类型
                    });

            //创建map对照表
            Dictionary<string, string> dicSalesIssueD = new Dictionary<string, string>();
            foreach (string key in salesIssueDColumns)
                dicSalesIssueD.Add(key, key);
            salesIssueDMap = UtilsClass.CreateBulkMapping(dicSalesIssueD);
            #endregion
        }

        /// <summary>
        /// 存储所需新增SalesIssue的数据集合的临时表
        /// </summary>
        private IDataEntityType CreateSalesIssueTmpTable(IQueryService qrySrv) {
            string typeName = "Temp_InsertSalesIssue_" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });

            IBusinessTypeService businessSrv = this.GetServiceForThisTypeKey<IBusinessTypeService>();
            SimplePropertyAttribute simplePrimaryAttri = businessSrv.SimplePrimaryKey;
            SimplePropertyAttribute qtyAttri = businessSrv.SimpleQuantity;
            SimplePropertyAttribute tempAttr;

            #region 字段
            //销货出口单主键
            defaultType.RegisterSimpleProperty("ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //单据类型
            defaultType.RegisterSimpleProperty("doc_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //单号
            defaultType.RegisterSimpleProperty("doc_no", businessSrv.SimpleDocNoType, string.Empty, false, new Attribute[] { businessSrv.SimpleDocNo });
            //工厂主键
            defaultType.RegisterSimpleProperty("plant_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //工厂的公司
            defaultType.RegisterSimpleProperty("company_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //人员
            defaultType.RegisterSimpleProperty("employee_no", businessSrv.SimpleBusinessCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleBusinessCode });
            //部门
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("picking_department_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //工厂编号
            defaultType.RegisterSimpleProperty("site_no", businessSrv.SimpleFactoryType, string.Empty, false, new Attribute[] { businessSrv.SimpleFactory });
            //信息批号
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });

            //20170619 add by zhangcn for P001-170606002 ===beigin===
            //单据顺序
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("DOC_Sequence", typeof(Int32), 0, false, new Attribute[] { tempAttr });

            //协同序号主键
            defaultType.RegisterSimpleProperty("GROUP_SYNERGY_D_ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            
            //20170619 add by zhangcn for P001-170606002 ===end===
            #endregion

            qrySrv.CreateTempTable(defaultType);

            return defaultType;
        }

        /// <summary>
        /// 存储所需新增SalesIssueD的数据集合的临时表
        /// </summary>
        private IDataEntityType CreateSalesIssueDTmpTable(IQueryService qrySrv) {
            string typeName = "Temp_InsertSalesIssueD_" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });

            IBusinessTypeService businessSrv = this.GetServiceForThisTypeKey<IBusinessTypeService>();
            SimplePropertyAttribute simplePrimaryAttri = businessSrv.SimplePrimaryKey;
            SimplePropertyAttribute qtyAttri = businessSrv.SimpleQuantity;
            SimplePropertyAttribute tempAttr;

            #region 字段
            //主键
            defaultType.RegisterSimpleProperty("SALES_ISSUE_D_ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //父主键
            defaultType.RegisterSimpleProperty("SALES_ISSUE_ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //信息批号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //序号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("SequenceNumber", typeof(int), 0, false, new Attribute[] { tempAttr });
            //品号
            defaultType.RegisterSimpleProperty("item_no", businessSrv.SimpleItemCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleItemCode });
            //特征码
            defaultType.RegisterSimpleProperty("item_feature_no", businessSrv.SimpleItemFeatureType, string.Empty, false, new Attribute[] { businessSrv.SimpleItemFeature });
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            //单位
            defaultType.RegisterSimpleProperty("picking_unit_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //单号
            defaultType.RegisterSimpleProperty("doc_no", businessSrv.SimpleDocNoType, string.Empty, false, new Attribute[] { businessSrv.SimpleDocNo });
            //来源序号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("seq", typeof(int), 0, false, new Attribute[] { tempAttr });
            //仓库
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("warehouse_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //库位
            defaultType.RegisterSimpleProperty("storage_spaces_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //批号
            defaultType.RegisterSimpleProperty("lot_no", businessSrv.SimpleLotCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleLotCode });
            //拣货数量
            defaultType.RegisterSimpleProperty("picking_qty", businessSrv.SimpleQuantityType, 0m, false, new Attribute[] { businessSrv.SimpleQuantity });
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            //条码编号
            defaultType.RegisterSimpleProperty("barcode_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //工厂编码
            defaultType.RegisterSimpleProperty("site_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //单据类型
            defaultType.RegisterSimpleProperty("doc_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            #endregion

            qrySrv.CreateTempTable(defaultType);

            return defaultType;
        }

        private DependencyObjectCollection QuerySalesSyneryFiD(object docNo) {
            QueryNode queryNode =
                OOQL.Select(1,
                            OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_FI_D_ID", "SALES_SYNERGY_FI_D_ID"),
                            OOQL.CreateProperty("SALES_SYNERGY_FI_D.SequenceNumber", "SequenceNumber"))
                    .From("SALES_SYNERGY.SALES_SYNERGY_FI_D", "SALES_SYNERGY_FI_D")
                    .InnerJoin("SALES_SYNERGY")
                    .On(OOQL.CreateProperty("SALES_SYNERGY.SALES_SYNERGY_ID") ==
                        OOQL.CreateProperty("SALES_SYNERGY_FI_D.SALES_SYNERGY_ID"))
                    .InnerJoin("SALES_DELIVERY")
                    .On(OOQL.CreateProperty("SALES_DELIVERY.GROUP_SYNERGY_ID.ROid") ==
                        OOQL.CreateProperty("SALES_SYNERGY.SALES_SYNERGY_ID"))
                    .Where(OOQL.CreateProperty("SALES_DELIVERY.DOC_NO") == OOQL.CreateConstants(docNo))
                    .OrderBy(new OrderByItem(OOQL.CreateProperty("SALES_SYNERGY_FI_D.SequenceNumber"), SortType.Desc));

            return this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        /// <summary>
        /// 单据生成结果结构
        /// </summary>
        public class DocumentInfo {
            /// <summary>
            /// ID
            /// </summary>
            public object ID { get; set; }
            /// <summary>
            /// 单据类型
            /// </summary>
            public object DOC_ID { get; set; }
            /// <summary>
            /// 组织
            /// </summary>
            public object OwnerOrgID { get; set; }
            /// <summary>
            /// 单号
            /// </summary>
            public string DOC_NO { get; set; }
        }

        /// <summary>
        /// 分组之后的实体行信息
        /// </summary>
        public class EntityLine {
            /// <summary>
            /// 决定唯一性的相关字段
            /// </summary>
            public string UniqueKey { get; set; }

            /// <summary>
            /// 行ID
            /// </summary>
            public object Key { get; set; }

            /// <summary>
            /// 行序号
            /// </summary>
            public int SequenceNumber { get; set; }
        }

        #endregion
    }
}
