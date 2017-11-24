//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/10 10:19:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>产生领退料单服务实现</description>
//----------------------------------------------------------------  
//20161213 modi by shenbao for B001-161213006 校验单据类型
//20161229 modi by shenbao for P001-161215001 增加来源为领料申请单
//20170209 modi by liwei1 for P001-170203001 修正单据类型取值
//20170310 modi by shenbao for B001-170309014 null值处理
//20170330 modi by wangrm for P001-170328001
//20170428 modi by wangyq for P001-170427001

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
    [ServiceClass(typeof(IInsertIssueReceiptService))]
    [Description("产生领退料单服务实现")]
    public sealed class InsertIssueReceiptService : ServiceComponent, IInsertIssueReceiptService {
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
                    _primaryKeySrv = this.GetService<IPrimaryKeyService>("ISSUE_RECEIPT");

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
                    _documentNumberGenSrv = this.GetService<IDocumentNumberGenerateService>("ISSUE_RECEIPT");

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
        /// 单据性质码
        /// </summary>
        private string _catagory;

        /// <summary>
        /// 记录每个单据类型对应的当前单号
        /// </summary>
        private Hashtable _currentDocNo = new Hashtable();

        #endregion

        #region IInsertIssueReceiptService 成员

        /// <summary>
        /// 产生领退料单
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型 1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collection">接口传入的领料单单身数据集合</param>
        public DependencyObjectCollection InsertIssueReceipt(string employeeNo, string scanType, DateTime reportDatetime, string pickingDepartmentNo
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
            DataTable issueReceipt = null, issueReceiptD = null;
            List<BulkCopyColumnMapping> issueReceiptMap = null, issueReceiptDMap = null;
            this.CreateRelateTable(ref issueReceipt, ref issueReceiptD,
                ref issueReceiptMap, ref issueReceiptDMap);

            List<DocumentInfo> listDocuments = new List<DocumentInfo>();  //记录生成的单据信息

            //组织数据BulkCopy需要的DataTable数据
            GetDocInfo(recommendedOperations, collection);
            if (_docInfos.Count <= 0)
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111275"));  //20161213 modi by shenbao for B001-161213006
            InsertDataTable(issueReceipt, issueReceiptD, collection, listDocuments, employeeNo, pickingDepartmentNo);
            if (issueReceipt.Rows.Count <= 0 || issueReceiptD.Rows.Count <= 0)  //没有数据值不再往下执行
                return rtnColl;

            #region 新增逻辑
            using (ITransactionService trans = this.GetService<ITransactionService>()) {
                IQueryService querySrv = this.GetService<IQueryService>();

                //新增临时表
                IDataEntityType issueReceiptTmp = CreateIssueReceiptTmpTable(querySrv);
                IDataEntityType issueReceiptDTmp = CreateIssueReceiptDTmpTable(querySrv);

                //批量新增到临时表
                querySrv.BulkCopy(issueReceipt, issueReceiptTmp.Name, issueReceiptMap.ToArray());
                querySrv.BulkCopy(issueReceiptD, issueReceiptDTmp.Name, issueReceiptDMap.ToArray());

                //利用临时表批量新增相关数据
                InsertIssueReceipt(querySrv, issueReceiptTmp, issueReceiptDTmp, reportDatetime, recommendedOperations);
                InsertIssueReceiptD(querySrv, issueReceiptDTmp, recommendedOperations);
                InsertBCLine(querySrv, issueReceiptDTmp);

                //EFNET签核
                var infos = listDocuments.GroupBy(c => new { c.DOC_ID, c.OwnerOrgID });
                string view = _catagory == "56" ? "ISSUE_RECEIPT.I01" : "ISSUE_RECEIPT.I02";
                foreach (var item in infos) {
                    IEFNETStatusStatusService efnetSrv = this.GetService<IEFNETStatusStatusService>();
                    efnetSrv.GetFormFlow(view, item.Key.DOC_ID, item.Key.OwnerOrgID,
                        item.Select(c => c.ID).ToArray());
                }

                //保存单据
                IReadService readSrv = GetService<IReadService>("ISSUE_RECEIPT");
                object[] entities = readSrv.Read(listDocuments.Select(c => c.ID).Distinct().ToArray());
                if (entities != null && entities.Length > 0) {
                    ISaveService saveSrv = this.GetService<ISaveService>("ISSUE_RECEIPT");
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
        private void GetDocInfo(string recommendedOperations, DependencyObjectCollection collection) {
            //20170209 add by liwei1 for P001-170203001 ===begin===
            if (collection.Count == 0) { return; }
            QueryNode selectNode = null;
            string infoLotNo = collection[0]["info_lot_no"].ToStringExtension(); //信息批号
            if (recommendedOperations == "7" || recommendedOperations == "8") {
                selectNode = OOQL.Select(1, OOQL.CreateProperty("MO.DOC_ID"))
                                    .From("MO", "MO")
                                    .Where((OOQL.AuthFilter("MO", "MO"))
                                        & (OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(infoLotNo)));
            } else {
                selectNode = OOQL.Select(1, OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_ID"))
                               .From("ISSUE_RECEIPT_REQ", "ISSUE_RECEIPT_REQ")
                               .Where((OOQL.AuthFilter("ISSUE_RECEIPT_REQ", "ISSUE_RECEIPT_REQ"))
                                   & (OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_NO") == OOQL.CreateConstants(infoLotNo)));
            }
            //20170209 add by liwei1 for P001-170203001 ===end===

            if (recommendedOperations.StartsWith("7"))
                _catagory = "56";
            else
                _catagory = "57";
            QueryNode node = OOQL.Select(true
                    , "PLANT.PLANT_CODE"
                    , "PLANT.PLANT_ID"
                    , "PARA_DOC_FIL.DOC_ID"
                    , "DOC.SEQUENCE_DIGIT"  //流水号位数
                    , "PARA_DOC_FIL.SOURCE_DOC_ID"//20170209 add by liwei1 for P001-170203001 
                )
                .From("PLANT", "PLANT")
                .InnerJoin("PARA_DOC_FIL")
                .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid"))
                .InnerJoin("DOC")
                .On(OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID") == OOQL.CreateProperty("DOC.DOC_ID")
                    & OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants(_catagory))
                .Where(OOQL.AuthFilter("PLANT", "PLANT")
                //& OOQL.CreateProperty("PLANT.PLANT_CODE").In(OOQL.CreateDyncParameter("CODES", collection.Select(c => c["site_no"].ToStringExtension()).ToArray())));20170209 mark by liwei1 for P001-170203001
                //20170209 add by liwei1 for P001-170203001 ===begin===
                       & ((OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(collection[0]["site_no"]))
                          & ((OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                             | (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == selectNode))))
                .OrderBy(
                    OOQL.CreateOrderByItem(
                        OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"), SortType.Desc));
            //20170209 add by liwei1 for P001-170203001 ===end===

            _docInfos = this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 组织更新IssueReceipt bcLine DataTable
        /// </summary>
        /// <param name="issueReceiptD"></param>
        private void InsertDataTable(DataTable issueReceipt, DataTable issueReceiptD
            , DependencyObjectCollection colls, List<DocumentInfo> documents, string employeeNo, string pickingDepartmentNo) {
            foreach (DependencyObject obj in colls) {
                #region 新增表issueReceipt结构
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
                DataRow dr = issueReceipt.NewRow();
                dr["ID"] = key;  //主键
                dr["doc_id"] = infoItem["DOC_ID"];  //单据类型
                dr["doc_no"] = docNo;  //单号
                dr["employee_no"] = employeeNo;  //人员
                dr["picking_department_no"] = pickingDepartmentNo;  //部门
                dr["site_no"] = obj["site_no"]; //工厂编号
                dr["info_lot_no"] = obj["info_lot_no"];  //信息批号
                dr["plant_id"] = infoItem["PLANT_ID"];  //工厂ID

                //记录单据信息
                doc.ID = key;
                doc.DOC_ID = infoItem["DOC_ID"];
                doc.OwnerOrgID = infoItem["PLANT_ID"];
                doc.DOC_NO = docNo;
                documents.Add(doc);

                issueReceipt.Rows.Add(dr);

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
                        #region 新增表issueReceiptD结构

                        //组织DataTable
                        dr = issueReceiptD.NewRow();
                        dr["ISSUE_RECEIPT_ID"] = key;  //父主键
                        if (!lineKeyDic.ContainsKey(uniqueKey)) {  //新的一组，重新生成行主键和行号
                            EntityLine line = new EntityLine();
                            line.UniqueKey = uniqueKey;
                            line.Key = PrimaryKeySrv.CreateId();
                            line.SequenceNumber = sequenceno++;

                            dr["ISSUE_RECEIPT_D_ID"] = line.Key;
                            dr["SequenceNumber"] = line.SequenceNumber;

                            lineKeyDic.Add(uniqueKey, line);
                        } else {  //已经存在的
                            dr["ISSUE_RECEIPT_D_ID"] = lineKeyDic[uniqueKey].Key;
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

                        issueReceiptD.Rows.Add(dr);
                        #endregion
                    }
                }
            }
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增领料单
        /// </summary>
        private void InsertIssueReceipt(IQueryService qrySrv, IDataEntityType tmpIssueReceipt, IDataEntityType issueReceiptDTmp, DateTime reportDatetime, string recommendedOperations) {//20170428 modi by wangyq for P001-170427001 添加参数单身临时表issueReceiptDTmp
            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            //20161229 add by shenbao for P001-161215001 ===begin===
            QueryProperty sourcePropertyRTK = null;
            QueryProperty sourcePropertyROid = null;
            //20170428 modi by wangyq for P001-170427001======================begin===================
            QueryProperty sourceIdRtk = null;
            QueryProperty sourceIdROid = null;
            if (recommendedOperations == "7-3") {
                sourcePropertyRTK = OOQL.CreateConstants("ISSUE_RECEIPT_REQ", "SOURCE_DOC_ID.RTK");
                sourcePropertyROid = OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_ID", "SOURCE_DOC_ID.ROid");
                sourceIdRtk = OOQL.CreateProperty("ISSUE_RECEIPT_REQ.SOURCE_ID.RTK", "SOURCE_ID_RTK");
                sourceIdROid = OOQL.CreateProperty("ISSUE_RECEIPT_REQ.SOURCE_ID.ROid", "SOURCE_ID_ROid");
            } else {
                sourcePropertyRTK = OOQL.CreateConstants("OTHER", "SOURCE_DOC_ID.RTK");
                sourcePropertyROid = OOQL.CreateConstants(Maths.GuidDefaultValue(), "SOURCE_DOC_ID.ROid");
                sourceIdRtk = OOQL.CreateProperty("moNode.SOURCE_ID_RTK", "SOURCE_ID_RTK");
                sourceIdROid = OOQL.CreateProperty("moNode.SOURCE_ID_ROid", "SOURCE_ID_ROid");
            }
            //if (recommendedOperations == "7-3")
            //    sourcePropertyRTK = OOQL.CreateConstants("ISSUE_RECEIPT_REQ", "SOURCE_DOC_ID.RTK");
            //else
            //    sourcePropertyRTK = OOQL.CreateConstants("OTHER", "SOURCE_DOC_ID.RTK");
            //if (recommendedOperations == "7-3")
            //    sourcePropertyROid = OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_ID", "SOURCE_DOC_ID.ROid");
            //else
            //    sourcePropertyROid = OOQL.CreateConstants(Maths.GuidDefaultValue(), "SOURCE_DOC_ID.ROid");
            //20170428 modi by wangyq for P001-170427001======================end===================
            //20161229 add by shenbao for P001-161215001 ===end===
            properties.AddRange(new QueryProperty[]{
                OOQL.CreateProperty("tmpTable.ID","ISSUE_RECEIPT_ID"),  //主键
                OOQL.CreateProperty("tmpTable.doc_id","DOC_ID"),  //单据类型
                OOQL.CreateConstants(reportDatetime,"DOC_DATE"),  //单据日期
                OOQL.CreateProperty("tmpTable.doc_no","DOC_NO"),  //单号
                OOQL.CreateConstants(_catagory,"CATEGORY"),  //单据性质码
                OOQL.CreateConstants(reportDatetime,"TRANSACTION_DATE"),//领退料日期
                OOQL.CreateConstants("PLANT", "Owner_Org.RTK"),  //组织
                Formulas.IsNull(OOQL.CreateProperty("tmpTable.plant_id"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org.ROid"),  //组织ID  //20170310 modi by shenbao for B001-170309014
                sourceIdRtk,//20170428 add by wangyq for P001-170427001 old:OOQL.CreateConstants("WORK_CENTER", "SOURCE_ID.RTK"),  //性质
                sourceIdROid,//20170428 add by wangyq for P001-170427001 old:OOQL.CreateConstants(Maths.GuidDefaultValue(), "SOURCE_ID.ROid"),  //工作中心
                sourcePropertyRTK,  //来源单据.类别  //20161229 modi by shenbao for P001-161215001
                sourcePropertyROid,  //来源单据  //20161229 modi by shenbao for P001-161215001
                Formulas.IsNull(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Dept"),  //领料部门  //20170310 modi by shenbao for B001-170309014
                Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Emp"),  //领料人员  //20170310 modi by shenbao for B001-170309014
            });
            #endregion

            //20170428 add by wangyq for P001-170427001======================begin===================
            QueryNode moNode = OOQL.Select(1, OOQL.CreateProperty("MO.SOURCE_ID.ROid", "SOURCE_ID_ROid"),
                                                        OOQL.CreateProperty("MO.SOURCE_ID.RTK", "SOURCE_ID_RTK"),
                                                        OOQL.CreateProperty("tmpTable.ID"))
                 .From(tmpIssueReceipt.Name, "tmpTable")
                 .InnerJoin(issueReceiptDTmp.Name, "tmpTable_D")
                 .On(OOQL.CreateProperty("tmpTable.ID") == OOQL.CreateProperty("tmpTable_D.ISSUE_RECEIPT_ID"))
                 .InnerJoin("MO", "MO")
                 .On(OOQL.CreateProperty("tmpTable_D.doc_no") == OOQL.CreateProperty("MO.DOC_NO"));
            //20170428 add by wangyq for P001-170427001======================end===================
            QueryNode insertNode = OOQL.Select(
                    properties
                )
                .From(tmpIssueReceipt.Name, "tmpTable")
                .LeftJoin("EMPLOYEE")
                .On(OOQL.CreateProperty("tmpTable.employee_no") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE"))
                .LeftJoin("ADMIN_UNIT")
                .On(OOQL.CreateProperty("tmpTable.picking_department_no") == OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE"))
                .LeftJoin("ISSUE_RECEIPT_REQ")  //20161229 add by shenbao for P001-161215001
                .On(OOQL.CreateProperty("tmpTable.info_lot_no") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_NO"));
            //20170428 add by wangyq for P001-170427001======================begin===================
            if (recommendedOperations != "7-3") {
                insertNode = ((JoinOnNode)insertNode).LeftJoin(moNode, "moNode")
                    .On(OOQL.CreateProperty("tmpTable.ID") == OOQL.CreateProperty("moNode.ID"));
            }
            //20170428 add by wangyq for P001-170427001======================end===================

            //执行插入
            UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "ISSUE_RECEIPT", insertNode, properties.Select(c => c.Alias).ToArray());
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增领料单身
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpBCLine"></param>
        private void InsertIssueReceiptD(IQueryService qrySrv, IDataEntityType tmpIssueReceiptD, string recommendedOperations) {
            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            //20161229 add by shenbao for P001-161215001 ===begin===
            QueryProperty sourceType = null;  //类型
            QueryProperty issueReceiptREQid = null;  //领料申请单
            if (recommendedOperations == "7-3")
                sourceType = OOQL.CreateConstants("2", "SOURCE_TYPE");
            else
                sourceType = OOQL.CreateConstants("1", "SOURCE_TYPE");
            if (recommendedOperations == "7-3")
                issueReceiptREQid = OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ISSUE_RECEIPT_REQ_D_ID", "ISSUE_RECEIPT_REQ_ID");
            else
                issueReceiptREQid = OOQL.CreateConstants(Maths.GuidDefaultValue(), "ISSUE_RECEIPT_REQ_ID");
            //20161229 add by shenbao for P001-161215001 ===end===
            properties.AddRange(new QueryProperty[]{
                OOQL.CreateProperty("tmpTable.SequenceNumber","SequenceNumber"),  //序号
                OOQL.CreateProperty("ITEM.ITEM_ID","ITEM_ID"),  //材料品号
                OOQL.CreateProperty("ITEM.ITEM_NAME","ITEM_DESCRIPTION"),  //材料品名
                Formulas.Case(null,OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.item_feature_no")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                },"ITEM_FEATURE_ID"),  //材料特征码
                OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION","ITEM_SPECIFICATION"),  //材料规格
                OOQL.CreateProperty("MO_D.ITEM_TYPE","ITEM_TYPE"),  //材料类型
                OOQL.CreateProperty("tmpTable.picking_qty", "ISSUE_RECEIPT_QTY"),  //领退料数量
                OOQL.CreateProperty("tmpTable.picking_qty", "ACTUAL_ISSUE_RECEIPT_QTY"),  //实际数量
                OOQL.CreateProperty("UNIT.UNIT_ID", "UNIT_ID"),  //单位
                Formulas.Ext("UNIT_CONVERT", "SECOND_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.SECOND_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //领料第二数量
                Formulas.Ext("UNIT_CONVERT", "ACTUAL_SECOND_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.SECOND_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //实际第二数量
                Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //领料库存数量
                Formulas.Ext("UNIT_CONVERT", "ACTUAL_INVENTORY_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //实际库存数量
                Formulas.IsNull(OOQL.CreateProperty("MO_D.OPERATION_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "OPERATION_ID"),  //工艺
                Formulas.IsNull(OOQL.CreateProperty("MO.MO_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "MO_ID"),  //工单
                OOQL.CreateProperty("tmpTable.picking_qty", "REPLACED_QTY"),  //被取替代数量
                Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),  //仓库
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("BIN.BIN_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.storage_spaces_no")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BIN_ID"),  //库位
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.lot_no")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"ITEM_LOT_ID"),  //批号
                OOQL.CreateProperty("MO_D.MO_D_ID", "MO_D_ID"),  //工单单身
                Formulas.Case(null, OOQL.CreateConstants("COST_DOMAIN"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(1),
                        OOQL.CreateConstants("COMPANY"))
                }, "COST_DOMAIN_ID_RTK"),  //成本域类型
                Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants(Maths.GuidDefaultValue()), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(1),
                        OOQL.CreateProperty("PLANT.COMPANY_ID")),
                    new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(2),
                        OOQL.CreateProperty("PLANT.COST_DOMAIN_ID")),
                    new CaseItem(OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL")==OOQL.CreateConstants(3),
                        OOQL.CreateProperty("WAREHOUSE.COST_DOMAIN_ID"))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()), "COST_DOMAIN_ID_ROid"),  //成本域
                OOQL.CreateConstants("MO","ISSUE_DESTINATION.RTK"),  //领料对象类型
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"ISSUE_DESTINATION.ROid"),  //领料对象
                OOQL.CreateConstants("","ISSUE_COMMENT"),  //领料说明
                OOQL.CreateConstants("","REMARK"),  //备注
                Formulas.IsNull(OOQL.CreateProperty("MO.MO_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"SOURCE_MO_ID"),  //源工单
                OOQL.CreateConstants("N","ApproveStatus"),  //审核码
                OOQL.CreateConstants(OrmDataOption.EmptyDateTime,"ApproveDate"),  //审核日期
                OOQL.CreateProperty("tmpTable.ISSUE_RECEIPT_D_ID","ISSUE_RECEIPT_D_ID"),  //主键
                OOQL.CreateProperty("tmpTable.ISSUE_RECEIPT_ID","ISSUE_RECEIPT_ID"),  //父主键
                Formulas.IsNull(OOQL.CreateProperty("MO_D.MO_D_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"REPLACED_MO_D_ID"),  //被替代工单备料
                sourceType,  //类型  //20161229 modi by shenbao for P001-161215001
                Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("DOC.BC_CHECK")==OOQL.CreateConstants("0"),
                        OOQL.CreateConstants("0"))
                }, "BC_CHECK_STATUS"),  //检核码
                issueReceiptREQid,  //领料申请单  //20161229 modi by shenbao for P001-161215001
                Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT")==OOQL.CreateConstants(false)
                        |OOQL.CreateProperty("tmpTable.picking_qty")==OOQL.CreateConstants(0,GeneralDBType.Decimal)
                        |OOQL.CreateProperty("MO_D.ITEM_TYPE")==OOQL.CreateConstants("3"),
                        OOQL.CreateConstants("0"))
                }, "SN_COLLECTED_STATUS")  //序列号检核码
            });
            #endregion

            QueryNode groupNode = GroupNode(tmpIssueReceiptD, true);
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
                .On(OOQL.CreateProperty("tmpTable.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"));
            //20161229 add by shenbao for P001-161215001 ===begin===
            if (recommendedOperations == "7-3") {
                insertNode = ((JoinOnNode)insertNode)
                .InnerJoin("ISSUE_RECEIPT_REQ")
                .On(OOQL.CreateProperty("tmpTable.doc_no") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_NO"))
                .InnerJoin("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_D", "ISSUE_RECEIPT_REQ_D")
                .On(OOQL.CreateProperty("tmpTable.seq") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.SequenceNumber")
                    & OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ISSUE_RECEIPT_REQ_ID"))
                .InnerJoin("MO")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.MO_ID") == OOQL.CreateProperty("MO.MO_ID"))
                .InnerJoin("MO.MO_D", "MO_D")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.MO_D_ID") == OOQL.CreateProperty("MO_D.MO_D_ID"));
            } else {
                insertNode = ((JoinOnNode)insertNode)
                .InnerJoin("MO")
                .On(OOQL.CreateProperty("tmpTable.doc_no") == OOQL.CreateProperty("MO.DOC_NO"))
                .InnerJoin("MO.MO_D", "MO_D")
                .On(OOQL.CreateProperty("tmpTable.seq") == OOQL.CreateProperty("MO_D.SequenceNumber")
                    & OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_D.MO_ID"));
            }
            //20161229 add by shenbao for P001-161215001 ===end===
            insertNode = ((JoinOnNode)insertNode)
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("tmpTable.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                    & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("tmpTable.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE"))
                .LeftJoin("ITEM_LOT")
                .On(OOQL.CreateProperty("tmpTable.lot_no") == OOQL.CreateProperty("ITEM_LOT.LOT_CODE")
                    & OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_ID")
                    & ((OOQL.CreateProperty("tmpTable.item_feature_no") == OOQL.CreateConstants("")
                        & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                        | (OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID"))))
                .InnerJoin("PARA_COMPANY")
                .On(OOQL.CreateProperty("PLANT.COMPANY_ID") == OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid"))
                .InnerJoin("DOC")
                .On(OOQL.CreateProperty("tmpTable.doc_id") == OOQL.CreateProperty("DOC.DOC_ID"));

            //执行插入
            UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "ISSUE_RECEIPT.ISSUE_RECEIPT_D", insertNode, properties.Select(c => c.Alias).ToArray());
        }

        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="tmpIssueReceiptD"></param>
        /// <returns></returns>
        public QueryNode GroupNode(IDataEntityType tmpIssueReceiptD, bool isEntityLine) {
            List<QueryProperty> properties = new List<QueryProperty>{OOQL.CreateProperty("TMP.ISSUE_RECEIPT_D_ID")
                    , OOQL.CreateProperty("TMP.ISSUE_RECEIPT_ID")
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
        /// 利用临时表关联实体表进行批量新增条码交易明细
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpBCLine"></param>
        private void InsertBCLine(IQueryService qrySrv, IDataEntityType tmpIssueReceiptD) {
            bool bcLintFlag = UtilsClass.IsBCLineManagement(qrySrv);
            if (!bcLintFlag)
                return;

            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                Formulas.NewId("BC_LINE_ID"),  //主键
                OOQL.CreateProperty("tmpTable.barcode_no","BARCODE_NO"),  //条码CODE
                OOQL.CreateConstants("ISSUE_RECEIPT.ISSUE_RECEIPT_D","SOURCE_ID.RTK"),  //来源单据类型
                OOQL.CreateProperty("tmpTable.ISSUE_RECEIPT_D_ID","SOURCE_ID.ROid"),  //来源单据
                Formulas.Ext("UNIT_CONVERT", "QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //数量
                Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID"),  //仓库
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("BIN.BIN_ID"),new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.storage_spaces_no")==OOQL.CreateConstants("")
                        ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BIN_ID")  //库位
                //20170330 add by wangrm for P001-170328001=====start=======
                , Formulas.IsNull(OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.RTK"),OOQL.CreateConstants(string.Empty), "Owner_Org.RTK")
                , Formulas.IsNull(OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.ROid"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org.ROid")
                , Formulas.IsNull(OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_DOC_ID")
                , Formulas.IsNull(OOQL.CreateProperty("ISSUE_RECEIPT.DOC_DATE"),OOQL.CreateConstants(OrmDataOption.EmptyDateTime), "DOC_DATE")
                //20170330 add by wangrm for P001-170328001=====end=======
            });
            #endregion

            QueryNode groupNode = GroupNode(tmpIssueReceiptD, false);
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
                .LeftJoin("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                .On(OOQL.CreateProperty("tmpTable.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID"))
                //20170330 add by wangrm for P001-170328001=====end=======
                 ;

            //执行插入
            UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "BC_LINE", insertNode, properties.Select(c => c.Alias).ToArray());
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
        private void CreateRelateTable(ref DataTable issueReceipt, ref DataTable issueReceiptD
            , ref List<BulkCopyColumnMapping> issueReceiptMap, ref List<BulkCopyColumnMapping> issueReceiptDMap) {
            #region 创建领料单表
            string[] issueReceiptColumns = new string[]{
                    "ID",  //主键
                    "doc_id",  //单据类型
                    "doc_no",  //单号
                    "plant_id",  //工厂id
                    "employee_no",  //人员
                    "picking_department_no",  //部门
                    "site_no",  //工厂编号
                    "info_lot_no"   //信息批号
            };
            issueReceipt = UtilsClass.CreateDataTable("ISSUE_RECEIPT", issueReceiptColumns,
                    new Type[]{
                        typeof(object),  //主键
                        typeof(object),  //单据类型
                        typeof(string),  //单号
                        typeof(object),  //工厂编号
                        typeof(string),  //人员
                        typeof(string),  //部门
                        typeof(string),  //工厂编号
                        typeof(string)   //信息批号
                    });

            //创建map对照表
            Dictionary<string, string> dicIssueReceipt = new Dictionary<string, string>();
            foreach (string key in issueReceiptColumns)
                dicIssueReceipt.Add(key, key);
            issueReceiptMap = UtilsClass.CreateBulkMapping(dicIssueReceipt);
            #endregion

            #region 创建领料单身表
            string[] issueReceiptDColumns = new string[]{
                    "ISSUE_RECEIPT_D_ID",  //主键
                    "ISSUE_RECEIPT_ID",  //父主键
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
            issueReceiptD = UtilsClass.CreateDataTable("ISSUE_RECEIPT_D", issueReceiptDColumns,
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
            Dictionary<string, string> dicIssueReceiptD = new Dictionary<string, string>();
            foreach (string key in issueReceiptDColumns)
                dicIssueReceiptD.Add(key, key);
            issueReceiptDMap = UtilsClass.CreateBulkMapping(dicIssueReceiptD);
            #endregion
        }

        /// <summary>
        /// 存储所需新增IssueReceipt的数据集合的临时表
        /// </summary>
        private IDataEntityType CreateIssueReceiptTmpTable(IQueryService qrySrv) {
            string typeName = "Temp_InsertIssueReceipt_" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });

            IBusinessTypeService businessSrv = this.GetServiceForThisTypeKey<IBusinessTypeService>();
            SimplePropertyAttribute simplePrimaryAttri = businessSrv.SimplePrimaryKey;
            SimplePropertyAttribute qtyAttri = businessSrv.SimpleQuantity;
            SimplePropertyAttribute tempAttr;

            #region 字段
            //领料单主键
            defaultType.RegisterSimpleProperty("ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //单据类型
            defaultType.RegisterSimpleProperty("doc_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //单号
            defaultType.RegisterSimpleProperty("doc_no", businessSrv.SimpleDocNoType, string.Empty, false, new Attribute[] { businessSrv.SimpleDocNo });
            //工厂主键
            defaultType.RegisterSimpleProperty("plant_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //人员
            defaultType.RegisterSimpleProperty("employee_no", businessSrv.SimpleBusinessCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleBusinessCode });
            //部门
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("picking_department_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //工厂编号
            defaultType.RegisterSimpleProperty("site_no", businessSrv.SimpleFactoryType, string.Empty, false, new Attribute[] { businessSrv.SimpleFactory });
            //信息批号
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            #endregion

            qrySrv.CreateTempTable(defaultType);

            return defaultType;
        }

        /// <summary>
        /// 存储所需新增IssueReceiptD的数据集合的临时表
        /// </summary>
        private IDataEntityType CreateIssueReceiptDTmpTable(IQueryService qrySrv) {
            string typeName = "Temp_InsertIssueReceiptD_" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });

            IBusinessTypeService businessSrv = this.GetServiceForThisTypeKey<IBusinessTypeService>();
            SimplePropertyAttribute simplePrimaryAttri = businessSrv.SimplePrimaryKey;
            SimplePropertyAttribute qtyAttri = businessSrv.SimpleQuantity;
            SimplePropertyAttribute tempAttr;

            #region 字段
            //主键
            defaultType.RegisterSimpleProperty("ISSUE_RECEIPT_D_ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //父主键
            defaultType.RegisterSimpleProperty("ISSUE_RECEIPT_ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
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
