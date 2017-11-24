//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-04-18</createDate>
//<description>生成到货检验单服务</description>
//---------------------------------------------------------------- 

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using Digiwin.ERP.PO_ARRIVAL_INSPECTION.Business;
using Digiwin.ERP.PO_QC_RESULT.Business;
using Digiwin.Common.Core;
using Digiwin.ERP.EFNET.Business;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 生成到货检验单服务
    /// </summary>
    [ServiceClass(typeof(IInsertPoArrivalInspectionService))]
    [Description("生成到货检验单服务")]
    public class InsertPoArrivalInspectionService : ServiceComponent, IInsertPoArrivalInspectionService {
        #region 接口方法

        /// <summary>
        /// 根据传入的信息，生成到货检验单信息
        /// </summary>
        /// <param name="delivery_no">送货单</param>
        /// <param name="supplier_no">供货商</param>
        /// <param name="supplier_name">供货商名称</param>
        /// <param name="receipt_no">收货单</param>
        /// <param name="receipt_list"></param>
        /// <returns></returns>
        public Hashtable InsertPoArrivalInspection(string delivery_no, string supplier_no, string supplier_name, string receipt_no,
            DependencyObjectCollection receipt_list) {
            try {
                // 参数检查
                IInfoEncodeContainer infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                if (Maths.IsEmpty(receipt_no)) {
                    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "receipt_no"));//‘入参【receipt_no】未传值’
                }

                //获取到货单信息
                DependencyObjectCollection purchaseArrival= GetPurchaseArrival(receipt_no);

                //单据性质检查
                Dictionary<int, object> docIdDt = new Dictionary<int, object>();
                foreach (DependencyObject item in purchaseArrival){
                    string category=string.Empty;
                    if (item["PURCHASE_TYPE"].ToStringExtension()=="1" || item["PURCHASE_TYPE"].ToStringExtension()=="5"){
                        category = "Q1";
                    } else if (item["PURCHASE_TYPE"].ToStringExtension() == "2" || item["PURCHASE_TYPE"].ToStringExtension() == "3") {
                        category = "Q2";                        
                    }
                    object  docId = GetDocId(receipt_no, category);
                    if (docId == null || Maths.IsEmpty(docId)) {
                        throw new BusinessRuleException(infoCodeSer.GetMessage("A111275"));//未找到对应的FIL单据类型设置，请检查！                  
                    }
                    docIdDt.Add(item["SequenceNumber"].ToInt32(), docId);
                }
                
                List<int> paiSequenceNumber = (from item in purchaseArrival where (!item["PURCHASE_TYPE"].Equals("3") && item["INSPECT_MODE_IP"].Equals("2")) || (item["PURCHASE_TYPE"].Equals("3") && item["INSPECT_MODE_MRD"].Equals("2")) select item["SequenceNumber"].ToInt32()).ToList();

                //生成到货检验逻辑
                string qcNo = InsertPoArrivalInspection(receipt_no, receipt_list, paiSequenceNumber, docIdDt);

                //组织返回结果
                Hashtable result = new Hashtable{{"qc_no", qcNo}};
                return result;
            } catch (Exception) {
                throw;
            }
        }

        private string InsertPoArrivalInspection(string receiptNo, DependencyObjectCollection receiptList, List<int> paiSequenceNumber, Dictionary<int, object> docIdDt) {
            //获取生成到货检验单基础数据
            DataTable poArrivalInspection = GetInsertPoArrivalInspection(receiptNo, paiSequenceNumber);
            //把查询数据以序号为Key，存入字典中。方便后期直接根据序号拿数据
            Dictionary<int, DataRow> poArrivalDictionary = new Dictionary<int, DataRow>();
            foreach (DataRow item in poArrivalInspection.Rows){
                if (!poArrivalDictionary.ContainsKey(item["SequenceNumber"].ToInt32())){
                    poArrivalDictionary.Add(item["SequenceNumber"].ToInt32(), item);
                }
            }
            //创建生成采购到货检验单缓存临时表
            DataTable poArrivalInspectionInfo = poArrivalInspection.Clone(); //复制表结构
            poArrivalInspectionInfo.TableName = "PO_ARRIVAL_INSPECTION";
            //创建生成采购到货检验单计数单身缓存临时表
            DataTable poArrivalInspectionDInfo = CreatePoArrivalInspectionDInfo();
            //创建生成采购到货检验单计量单身缓存临时表
            DataTable poArrivalInspectionD1Info = CreatePoArrivalInspectionD1Info();
            //创建计数品质检验不良原因缓存临时表
            DataTable defectiveReasonsAiId = CreateDefectiveReasonsAiIdInfo();
            
            IPrimaryKeyService keyService = GetService<IPrimaryKeyService>(TypeKey); //生成主键服务 
            IDocumentNumberGenerateService docNumService =GetService<IDocumentNumberGenerateService>("PO_ARRIVAL_INSPECTION"); //自动编号服务  
            ICreateInspectionService createInspectionService = GetService<ICreateInspectionService>("PO_ARRIVAL_INSPECTION");//顺序检查服务
            IGenerateOrDeletePurchaseArivalQCResultService generatePurchaseArivalQc =GetService<IGenerateOrDeletePurchaseArivalQCResultService>("PO_QC_RESULT");//生成/删除采购质检业务信息服务

            IItemQtyConversionService itemQtyConversionService = GetService<IItemQtyConversionService>(TypeKey);
            ILogOnService loginService = GetService<ILogOnService>();
            List<DocmentInfo> listDocuments = new List<DocmentInfo>();
            Dictionary<string, string> salesReturnDocNo = new Dictionary<string, string>(); //记录产生的单号用于批量生成的时候防止重复单号
            const decimal defaultDecimal = 0m;
            string resutQcNo = string.Empty;//返回结果单号
            //循环入参数据，分别给到货检验单、货检验单计数单身、货检验单计量单身身临时表插入数据
            foreach (DependencyObject receiptListItem in receiptList){
                object poArrivalInspectionId = keyService.CreateId(); //主键
                if (poArrivalDictionary.ContainsKey(receiptListItem["seq"].ToInt32())) {
                    #region 到货检验单临时表插入数据
                    //获取生单基础数据
                    DataRow dr = poArrivalDictionary[receiptListItem["seq"].ToInt32()];

                    //获取单据性质
                    object docId = Maths.GuidDefaultValue();
                    if (docIdDt.ContainsKey(dr["SequenceNumber"].ToInt32())){
                        docId = docIdDt[dr["SequenceNumber"].ToInt32()];
                    }
                    if (dr["PURCHASE_TYPE"].ToStringExtension() == "1" || dr["PURCHASE_TYPE"].ToStringExtension() == "5") {
                        dr["CATEGORY"] = "Q1";
                    } else if (dr["PURCHASE_TYPE"].ToStringExtension() == "2" || dr["PURCHASE_TYPE"].ToStringExtension() == "3") {
                        dr["CATEGORY"] = "Q2";
                    }

                    //计算列赋值
                    dr["PO_ARRIVAL_INSPECTION_ID"] = poArrivalInspectionId; //主键
                    dr["DOC_ID"] = docId; //单据类型
                    dr["DOC_DATE"] = DateTime.Now.Date; //单据日期

                    dr["DOC_NO"] = docNumService.NextNumber(dr["DOC_ID"], dr["DOC_DATE"].ToDate()); //单号
                    if (!salesReturnDocNo.ContainsKey(dr["DOC_NO"].ToString())){
                        salesReturnDocNo.Add(dr["DOC_NO"].ToString(), dr["DOC_NO"].ToString());
                    }
                    else{
                        string oldDocNo = salesReturnDocNo[dr["DOC_NO"].ToString()]; //取得字典中存储的最大单号
                        string docNo = NewDocNo(oldDocNo); //产生新单号
                        salesReturnDocNo[dr["DOC_NO"].ToString()] = docNo; //更新字段中单号
                        dr["DOC_NO"] = docNo; //当前笔单号重新赋值
                    }

                    object[] createInspection = createInspectionService.GetInspectionId(dr["ITEM_ID"],
                        dr["Owner_Org_ROid"],
                        dr["ITEM_FEATURE_ID"], dr["OPERATION_ID"]);
                    if (!Maths.IsEmpty(createInspection)){
                        dr["INSPECTION_PLAN_ID"] = createInspection[0]; //质检方案ID                        
                        dr["INSPECTION_TIMES"] = createInspection[1]; //最大抽样次数                        
                    }

                    dr["INSPECTION_QTY"] = receiptListItem["receipt_qty"]; //送检数量
                    dr["INSPECTION_DUE_DATE"] = Maths.AddTimeValue(dr["ARRIVAL_DATE"].ToDate(), 1,
                        dr["INSPECT_DAYS"].ToInt32()); //检验期限
                    dr["INVENTORY_QTY"] = itemQtyConversionService.GetConvertedQty(dr["ITEM_ID"],
                        dr["INSPECTION_UNIT_ID"], dr["INSPECTION_QTY"].ToDecimal(), dr["STOCK_UNIT_ID"]); //库存单位数量
                    dr["SECOND_QTY"] = itemQtyConversionService.GetConvertedQty(dr["ITEM_ID"], dr["INSPECTION_UNIT_ID"],
                        dr["INSPECTION_QTY"].ToDecimal(), dr["SECOND_UNIT_ID"]); //库存单位数量
                    dr["ACCEPTABLE_QTY"] = receiptListItem["ok_qty"]; //送检数量
                    dr["UNQUALIFIED_QTY"] = receiptListItem["unqualified_qty"]; //不合格数量
                    dr["DESTROYED_QTY"] = receiptListItem["checkdestroy_qty"]; //破坏数量
                    //判定结果
                    if (receiptListItem["result_type"].ToStringExtension() == "Y"){
                        dr["DECISION"] = "1";
                    }
                    else{
                        dr["DECISION"] = "2";
                    }

                    //后道工序
                    if (!Maths.IsEmpty(dr["MO_ROUTING_D_ID"])){
                        DependencyObject moRouting = GetMoRouting(dr["MO_ROUTING_D_ID"]);
                        dr["TO_MO_ROUTING_D_ID"] = moRouting["MO_ROUTING_D_ID"];
                        dr["TO_OPERATION_ID"] = moRouting["OPERATION_ID"];
                    }

                    dr["Owner_Emp"] = GetEmployeeInfo(loginService.CurrentUserId);
                    dr["Owner_Dept"] = GetAdminUnitInfo(dr["Owner_Emp"]);
                    //系统管理字段
                    AddManagementData(dr, loginService);
                    poArrivalInspectionInfo.Rows.Add(dr.ItemArray);

                    #endregion

                    #region 到货检验单计数单身临时表插入数据

                    DependencyObjectCollection qcList = receiptListItem["qc_list"] as DependencyObjectCollection;
                    if (qcList == null) continue; //不存在qc_list集合数据跳出循环
                    foreach (var qcListItem in qcList){
                        if (qcListItem["qc_seq"].ToInt32() == 0) continue;//序号为0的数据是默认数据只是为了json结构完整插入的，虚无插入E10系统
                        DataRow qcListDr = poArrivalInspectionDInfo.NewRow();
                        qcListDr["PO_ARRIVAL_INSPECTION_D_ID"] = keyService.CreateId(); //主键
                        qcListDr["ParentId"] = poArrivalInspectionId; //父主键
                        qcListDr["SEQUENCE"] = qcListItem["qc_seq"];
                        qcListDr["DEFECT_CLASS"] = qcListItem["defect_level"];
                        qcListDr["INSPECTION_QTY"] = qcListItem["test_qty"];
                        qcListDr["INSPECTION_QQ"] = qcListItem["return_qty"];
                        qcListDr["INSPECTION_ITEM_ID"] = GetInspectionItem(qcListItem["test_no"], dr["Owner_Org_ROid"]);
                        qcListDr["INSPECTION_AC"] = qcListItem["acceptable_qty"];
                        qcListDr["INSPECTION_RE"] = qcListItem["rejected_qty"];
                        if (receiptListItem["result_type"].ToStringExtension() == "Y") {
                            qcListDr["DECISION"] = true;
                        } else {
                            qcListDr["DECISION"] = false;
                        }
                        qcListDr["IMPACT_RESULT"] = true;
                        qcListDr["REMARK"] = string.Empty;

                        qcListDr["DEFECTIVE_REASONS_ID"] = Maths.GuidDefaultValue();
                        //系统管理字段
                        AddManagementData(qcListDr, loginService);
                        

                        DependencyObjectCollection reasonlist = qcListItem["reason_list"] as DependencyObjectCollection;
                        if (reasonlist == null ) continue;
                        //defect_qty为0且reason_no为空的数据是默认数据只是为了json结构完整插入的，不需要插入E10系统
                        if (reasonlist.Count == 1 && (!Maths.IsEmpty(reasonlist[0]["reason_no"]) || reasonlist[0]["defect_qty"].ToInt32() != 0)) {
                            qcListDr["DEFECTIVE_REASONS_ID"] = GetDefectiveReasonsId(reasonlist[0]["reason_no"].ToStringExtension());
                        }
                        else{
                            //defect_qty为0且reason_no为空的数据是默认数据只是为了json结构完整插入的，不需要插入E10系统
                            if (!Maths.IsEmpty(reasonlist[0]["reason_no"]) || reasonlist[0]["defect_qty"].ToInt32() != 0){
                                foreach (DependencyObject reasonlistItem in reasonlist){
                                    DataRow reasonlistDr = defectiveReasonsAiId.NewRow();
                                    reasonlistDr["DEFECTIVE_REASONS_AI_ID"] = keyService.CreateId(); //主键
                                    reasonlistDr["ParentId"] = qcListDr["PO_ARRIVAL_INSPECTION_D_ID"]; //父主键
                                    reasonlistDr["DEFECTS"] = reasonlistItem["defect_qty"];
                                    reasonlistDr["REMARK"] = string.Empty;
                                    reasonlistDr["DEFECTIVE_REASONS_ID"] =
                                        GetDefectiveReasonsId(reasonlistItem["reason_no"].ToStringExtension());
                                    //系统管理字段
                                    AddManagementData(reasonlistDr, loginService);
                                    defectiveReasonsAiId.Rows.Add(reasonlistDr.ItemArray);

                                    //子单身第一笔时把不良原因主键赋值给单身不良原因
                                    if (reasonlist.IndexOf(reasonlistItem)==0){
                                        qcListDr["DEFECTIVE_REASONS_ID"] = reasonlistDr["DEFECTIVE_REASONS_ID"];
                                    }
                                }
                            }
                        }
                        //把对应数据添加至预生单缓存中
                        poArrivalInspectionDInfo.Rows.Add(qcListDr.ItemArray);
                    }

                    #endregion

                    #region 到货检验单计量单身临时表插入数据

                    DependencyObjectCollection collection =
                        createInspectionService.CreateInspection(dr["INSPECTION_PLAN_ID"], dr["OPERATION_ID"]);
                    foreach (DependencyObject item in collection){
                        if (item["INSPECTION_TYPE"].Equals("VI")){//collection.检验类型=‘VI’
                            DataRow itemDr = poArrivalInspectionD1Info.NewRow();
                            itemDr["PO_ARRIVAL_INSPECTION_D1_ID"] = keyService.CreateId();
                            itemDr["ParentId"] = poArrivalInspectionId;
                            itemDr["SEQUENCE"] = item["SEQUENCE"]; //检验顺序
                            DependencyObject inspectionPlantD = GetInspectionPlantD(dr["INSPECTION_PLAN_ID"],
                                item["INSPECTION_ITEM_ID"]);
                            itemDr["DEFECT_CLASS"] = inspectionPlantD["DEFECT_CLASS"];
                            itemDr["INSPECTION_QTY"] = defaultDecimal;
                            itemDr["INSPECTION_QQ"] = defaultDecimal;
                            itemDr["DECISION"] = false;
                            itemDr["IMPACT_RESULT"] = inspectionPlantD["IMPACT_RESULT"];
                            itemDr["REMARK"] = string.Empty;
                            itemDr["INSPECTION_ITEM_ID"] = item["INSPECTION_ITEM_ID"]; //检验项目
                            itemDr["DEFECTIVE_REASONS_ID"] = Maths.GuidDefaultValue();
                            itemDr["ACCEPTANCE_CONSTANT"] = defaultDecimal;
                            itemDr["SS"] = defaultDecimal;
                            itemDr["XX"] = defaultDecimal;
                            //系统管理字段
                            AddManagementData(itemDr, loginService);
                            poArrivalInspectionD1Info.Rows.Add(itemDr.ItemArray);
                        }
                    }

                    #endregion

                    //记录单据信息
                    DocmentInfo docmentInfo = new DocmentInfo();
                    docmentInfo.Id = dr["PO_ARRIVAL_INSPECTION_ID"];
                    docmentInfo.OwnerOrgId = dr["Owner_Org_ROid"];
                    docmentInfo.DocId = dr["DOC_ID"];
                    docmentInfo.DocNo = dr["DOC_NO"].ToStringExtension();
                    listDocuments.Add(docmentInfo);

                    if (Maths.IsEmpty(resutQcNo)){
                        resutQcNo = dr["DOC_NO"].ToStringExtension();
                    }
                    else{
                        resutQcNo += "，" + dr["DOC_NO"].ToStringExtension();
                    }
                }
            }
            #region 将数据统一插入数据库中

            using (ITransactionService trans = GetService<ITransactionService>()) {
                IQueryService querySrv = GetService<IQueryService>();
                //生成采购到货检验单
                if (poArrivalInspectionInfo.Rows.Count > 0) {
                    poArrivalInspectionInfo.Columns.Remove("SequenceNumber");
                    poArrivalInspectionInfo.Columns.Remove("ARRIVAL_DATE");
                    poArrivalInspectionInfo.Columns.Remove("INSPECT_DAYS");
                    poArrivalInspectionInfo.Columns.Remove("STOCK_UNIT_ID");
                    poArrivalInspectionInfo.Columns.Remove("SECOND_UNIT_ID");
                    poArrivalInspectionInfo.Columns.Remove("PURCHASE_TYPE");
                    BulkCopyAll(poArrivalInspectionInfo, querySrv);
                }
                //生成采购到货检验单计数单身
                if (poArrivalInspectionDInfo.Rows.Count > 0) {
                    BulkCopyAll(poArrivalInspectionDInfo, querySrv);
                }
                //生成采购到货检验单计量单身
                if (poArrivalInspectionD1Info.Rows.Count > 0) {
                    BulkCopyAll(poArrivalInspectionD1Info, querySrv);
                }
                //生成计数品质检验不良原因
                if (defectiveReasonsAiId.Rows.Count > 0) {
                    BulkCopyAll(defectiveReasonsAiId, querySrv);
                }

                //EFNET签核
                var infos = listDocuments.GroupBy(c => new { DOC_ID = c.DocId, OwnerOrgID = c.OwnerOrgId });
                foreach (var item in infos) {
                    IEFNETStatusStatusService efnetSrv = GetService<IEFNETStatusStatusService>();
                    efnetSrv.GetFormFlow("PO_ARRIVAL_INSPECTION.I01", item.Key.DOC_ID, item.Key.OwnerOrgID,
                         item.Select(c => c.Id).ToArray());
                }

                //保存单据
                IReadService readSrv = GetService<IReadService>("PO_ARRIVAL_INSPECTION");
                object[] entities = readSrv.Read(listDocuments.Select(c => c.Id).Distinct().ToArray());
                if (entities != null && entities.Length > 0) {
                    ISaveService saveSrv = GetService<ISaveService>("PO_ARRIVAL_INSPECTION");
                    saveSrv.Save(entities);
                }

                trans.Complete();
            }

            #endregion
            return resutQcNo;
        }

        /// <summary>
        /// 单据生成结果结构
        /// </summary>
        public class DocmentInfo {
            /// <summary>
            /// ID
            /// </summary>
            public object Id { get; set; }
            /// <summary>
            /// 单据类型
            /// </summary>
            public object DocId { get; set; }
            /// <summary>
            /// 组织
            /// </summary>
            public object OwnerOrgId { get; set; }
            /// <summary>
            /// 单号
            /// </summary>
            public string DocNo { get; set; }
        }

        private void BulkCopyAll(DataTable dt, IQueryService querySrv){
            //生单列类型比较测试
            Dictionary<string, Type> columnsDt = new Dictionary<string, Type>();
            foreach (DataColumn item in dt.Columns) {
                if (!columnsDt.ContainsKey(item.ColumnName)) {
                    columnsDt.Add(item.ColumnName, item.DataType);
                }
            }
            //创建map对照表
            List<BulkCopyColumnMapping> consignTransferOutMap = GetBulkCopyColumnMapping(dt.Columns);
            //批量新增到临时表
            querySrv.BulkCopy(dt, dt.TableName, consignTransferOutMap.ToArray());
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

        /// <summary>
        ///  获取不良原因.主键
        /// </summary>
        /// <param name="dependencyObject"></param>
        /// <returns></returns>
        private object GetDefectiveReasonsId(string dependencyObject){
            QueryNode node =
                OOQL.Select(
                    OOQL.CreateProperty("DEFECTIVE_REASONS.DEFECTIVE_REASONS_ID"))
                    .From("DEFECTIVE_REASONS", "DEFECTIVE_REASONS")
                    .Where(OOQL.AuthFilter("DEFECTIVE_REASONS", "DEFECTIVE_REASONS")
                            &(OOQL.CreateProperty("DEFECTIVE_REASONS.DEFECTIVE_REASONS_CODE") ==OOQL.CreateConstants(dependencyObject)));
            object  defectiveReasonsId=GetService<IQueryService>().ExecuteScalar(node);
            return Maths.IsNotNull(defectiveReasonsId) ? defectiveReasonsId : Maths.GuidDefaultValue();
        }

        /// <summary>
        /// 增加管理字段数据
        /// </summary>
        /// <param name="dr"></param>
        /// <param name="loginService"></param>
        private void AddManagementData(DataRow dr, ILogOnService loginService) {
            dr["CreateBy"] = loginService.CurrentUserId;
            dr["CreateDate"] = DateTime.Now;
            dr["ModifiedBy"] = loginService.CurrentUserId;
            dr["ModifiedDate"] = DateTime.Now;
            dr["LastModifiedBy"] = loginService.CurrentUserId;
            dr["LastModifiedDate"] = DateTime.Now;
        }

        private DependencyObject GetInspectionPlantD(object inspectionPlanId,object inspectionItemId) {
            QueryNode node=
                OOQL.Select(1,
                    OOQL.CreateProperty("ipd.DEFECT_CLASS", "DEFECT_CLASS"),
                    OOQL.CreateProperty("ipd.IMPACT_RESULT", "IMPACT_RESULT"))
                    .From("INSPECTION_PLAN.INSPECTION_PLAN_D1", "ipd")
                    .Where((OOQL.AuthFilter("INSPECTION_PLAN.INSPECTION_PLAN_D1", "ipd"))
                           & (OOQL.CreateProperty("ipd.INSPECTION_PLAN_ID") == OOQL.CreateConstants(inspectionPlanId))
                           & (OOQL.CreateProperty("ipd.INSPECTION_ITEM_ID") == OOQL.CreateConstants(inspectionItemId)));
            DependencyObjectCollection inspectionPlantD= GetService<IQueryService>().ExecuteDependencyObject(node);
            return inspectionPlantD.Count > 0? inspectionPlantD[0]: new DependencyObject(inspectionPlantD.ItemDependencyObjectType);
        }

        /// <summary>
        /// 获取检验项目Id
        /// </summary>
        /// <param name="testNo"></param>
        /// <param name="plantId"></param>
        /// <returns></returns>
        private object GetInspectionItem(object testNo, object plantId) {
            QueryNode node =
                OOQL.Select(
                    OOQL.CreateProperty("ii.INSPECTION_ITEM_ID", "INSPECTION_ITEM_ID"))
                    .From("INSPECTION_ITEM", "ii")
                    .Where((OOQL.AuthFilter("INSPECTION_ITEM", "ii"))
                           &(OOQL.CreateProperty("ii.INSPECTION_ITEM_CODE") == OOQL.CreateConstants(testNo))
                           &(OOQL.CreateProperty("ii.Owner_Org.ROid") ==OOQL.CreateConstants(plantId)));
            return GetService<IQueryService>().ExecuteScalar(node) ?? Maths.GuidDefaultValue();
        }

        /// <summary>
        /// 获取后道工序、工艺
        /// </summary>
        /// <param name="moRoutingDId"></param>
        /// <returns></returns>
        private DependencyObject GetMoRouting(object moRoutingDId) {
            QueryNode node =
                OOQL.Select(1,
                            OOQL.CreateProperty("mrd.MO_ROUTING_D_ID", "MO_ROUTING_D_ID"),
                            OOQL.CreateProperty("mrd.OPERATION_ID", "OPERATION_ID"))
                        .From("MO_ROUTING.MO_ROUTING_D", "mrd")
                        .InnerJoin("OPERATION", "o")
                        .On((OOQL.CreateProperty("o.OPERATION_ID") == OOQL.CreateProperty("mrd.OPERATION_ID")))
                        .Where((OOQL.AuthFilter("MO_ROUTING.MO_ROUTING_D", "mrd"))
                            & OOQL.Exists(
                                    OOQL.Select(
                                        OOQL.CreateConstants(1, GeneralDBType.Int32, "IS_OK"))
                                        .From("MO_ROUTING.MO_ROUTING_D.MO_ROUTING_PATH", "mrp")
                                        .InnerJoin("MO_ROUTING.MO_ROUTING_D", "mrd2")
                                        .On((OOQL.CreateProperty("mrd2.MO_ROUTING_D_ID") ==
                                             OOQL.CreateProperty("mrp.MO_ROUTING_D_ID")))
                                        .Where((OOQL.CreateProperty("mrp.MO_ROUTING_D_ID") ==OOQL.CreateConstants(moRoutingDId))
                                               &(OOQL.CreateProperty("mrp.TO_SEQ") ==OOQL.CreateProperty("mrd.OPERATION_SEQ"))
                                               &(OOQL.CreateProperty("mrd.MO_ROUTING_ID") ==OOQL.CreateProperty("mrd2.MO_ROUTING_ID")))));
            DependencyObjectCollection moRoutings = GetService<IQueryService>().ExecuteDependencyObject(node);
            return moRoutings.Count > 0 ? moRoutings[0] : new DependencyObject(moRoutings.ItemDependencyObjectType);
        }

        /// <summary>
        /// 获取登录人员的员工信息
        /// </summary>
        /// <param name="employeeId">员工ID</param>
        /// <returns></returns>
        public object GetAdminUnitInfo(object employeeId) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("EMPLOYEE_D.ADMIN_UNIT_ID"))
                .From("EMPLOYEE.EMPLOYEE_D", "EMPLOYEE_D")
                .Where((OOQL.AuthFilter("EMPLOYEE.EMPLOYEE_D", "EMPLOYEE_D"))
                & OOQL.CreateProperty("EMPLOYEE_D.EMPLOYEE_ID") == OOQL.CreateConstants(employeeId)
                & (OOQL.CreateProperty("EMPLOYEE_D.IS_PRIMARY") == OOQL.CreateConstants(true)));
            object adminUnitId = GetService<IQueryService>().ExecuteScalar(node);
            return Maths.IsEmpty(adminUnitId) ? Maths.GuidDefaultValue():adminUnitId;
        }

        /// <summary>
        /// 获取登录人员的员工信息
        /// </summary>
        /// <param name="userId">用户ID</param>
        /// <returns></returns>
        public object GetEmployeeInfo(object userId) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("USER.EMPLOYEE_ID"))
                .From("USER", "USER")
                .Where((OOQL.AuthFilter("USER", "USER"))
                &(OOQL.CreateProperty("USER.USER_ID") == OOQL.CreateConstants(userId)));
            object adminUnitId = GetService<IQueryService>().ExecuteScalar(node);
            return Maths.IsEmpty(adminUnitId) ? Maths.GuidDefaultValue() : adminUnitId;
        }

        /// <summary>
        /// 产生新单号
        /// </summary>
        /// <param name="docNo">原始单号</param>
        /// <returns>新单号</returns>
        private string NewDocNo(string docNo) {
            //获取单号流水号，并加1
            int number = docNo.Remove(0, docNo.Length - 4).ToInt32() + 1;
            //新流水号补位
            string newNumber = number.ToStringExtension().PadLeft(4, '0');
            //获取新单号前面一部分
            string stratDocNo = docNo.Substring(0, docNo.Length - 4);
            //组合新单号，并返回
            return stratDocNo + newNumber;
        }

        /// <summary>
        /// 获取生成到货检验单基础数据
        /// </summary>
        /// <param name="receiptNo">到货单号</param>
        /// <param name="paiSequenceNumber">单身序号集合</param>
        /// <returns></returns>
        private DataTable GetInsertPoArrivalInspection(string receiptNo, List<int> paiSequenceNumber){
            string stringDefault = string.Empty;
            object guidDefault = Maths.GuidDefaultValue();
            const decimal decimalDefault = 0m;
            QueryNode node =
                OOQL.Select(true,
                            OOQL.CreateProperty("pad.SequenceNumber", "SequenceNumber"),
                            OOQL.CreateProperty("pa.ARRIVAL_DATE", "ARRIVAL_DATE"),
                            OOQL.CreateProperty("ip.INSPECT_DAYS", "INSPECT_DAYS"),
                            OOQL.CreateProperty("i.STOCK_UNIT_ID", "STOCK_UNIT_ID"),
                            OOQL.CreateProperty("i.SECOND_UNIT_ID", "SECOND_UNIT_ID"),
                            OOQL.CreateProperty("pad.PURCHASE_TYPE","PURCHASE_TYPE"),

                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid, "PO_ARRIVAL_INSPECTION_ID"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid,  "DOC_ID"),
                            OOQL.CreateConstants("Q1", "CATEGORY"),
                            OOQL.CreateProperty("pa.DOC_DATE", "DOC_DATE"),
                            OOQL.CreateConstants(stringDefault, GeneralDBType.String, "DOC_NO"),
                            Formulas.GetDate("INSPECTION_DATE"),
                            OOQL.CreateConstants(stringDefault, GeneralDBType.String, "INSPECTION_TIMES"),
                            OOQL.CreateProperty("pad.ITEM_ID", "ITEM_ID"),
                            OOQL.CreateProperty("pad.ITEM_DESCRIPTION", "ITEM_DESCRIPTION"),
                            OOQL.CreateProperty("pad.ITEM_FEATURE_ID", "ITEM_FEATURE_ID"),
                            OOQL.CreateProperty("pad.ITEM_SPECIFICATION", "ITEM_SPECIFICATION"),
                            OOQL.CreateProperty("pad.ITEM_LOT_ID", "ITEM_LOT_ID"),
                            OOQL.CreateProperty("pa.SUPPLIER_ID", "SUPPLIER_ID"),
                            Formulas.Cast(OOQL.CreateConstants(decimalDefault), GeneralDBType.Decimal, 16, 6, "INSPECTION_QTY"),
                            OOQL.CreateProperty("pad.BUSINESS_UNIT_ID", "INSPECTION_UNIT_ID"),
                            OOQL.CreateProperty("isdd.STRICTNESS_DEGREE", "STRICTNESS_DEGREE"),
                            Formulas.GetDate("INSPECTION_DUE_DATE"),
                            OOQL.CreateProperty("pa.Owner_Dept", "SUBMIT_DEPT_ID"),
                            OOQL.CreateProperty("pa.Owner_Emp", "SUBMIT_EMP_ID"),
                            Formulas.Cast(OOQL.CreateConstants(decimalDefault), GeneralDBType.Decimal, 16, 6, "INVENTORY_QTY"),
                            Formulas.Cast(OOQL.CreateConstants(decimalDefault), GeneralDBType.Decimal, 16, 6, "SECOND_QTY"),
                            Formulas.Cast(OOQL.CreateConstants(decimalDefault), GeneralDBType.Decimal, 16, 6, "ACCEPTABLE_QTY"),
                            Formulas.Cast(OOQL.CreateConstants(decimalDefault), GeneralDBType.Decimal, 16, 6, "UNQUALIFIED_QTY"),
                            Formulas.Cast(OOQL.CreateConstants(decimalDefault), GeneralDBType.Decimal, 16, 6, "DESTROYED_QTY"),
                            OOQL.CreateConstants(stringDefault, GeneralDBType.String, "DECISION"),
                            OOQL.CreateConstants(stringDefault, GeneralDBType.String, "DECISION_DESCRIPTION"),
                            OOQL.CreateConstants("1", GeneralDBType.String, "RESULT_STATUS"),
                            OOQL.CreateConstants(stringDefault, GeneralDBType.String, "REMARK"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid, "INSPECTION_PLAN_ID"),
                            OOQL.CreateProperty("p.COMPANY_ID", "COMPANY_ID"),
                            OOQL.CreateProperty("pad.PURCHASE_ARRIVAL_D_ID", "SOURCE_ID"),
                            Formulas.Case(null,
                                OOQL.CreateConstants(guidDefault, GeneralDBType.String),
                                OOQL.CreateCaseArray(
                                    OOQL.CreateCaseItem(
                                        (OOQL.CreateProperty("pad.PURCHASE_TYPE") ==OOQL.CreateConstants("3")),
                                        OOQL.CreateProperty("pad.REFERENCE_SOURCE_ID.ROid"))), "MO_ROUTING_D_ID"),
                            OOQL.CreateProperty("pad.OPERATION_ID", "OPERATION_ID"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid,  "TO_MO_ROUTING_D_ID"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid,  "TO_OPERATION_ID"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid,  "PROJECT_ID"),
                            OOQL.CreateConstants(false, GeneralDBType.Boolean, "DEDUCT_ARRIVED_QTY"),
                            OOQL.CreateProperty("pa.RECEIVE_Owner_Org.ROid", "Owner_Org_ROid"),
                            OOQL.CreateConstants("PLANT", "Owner_Org_RTK"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid,  "Owner_Emp"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid,  "Owner_Dept"),
                            OOQL.CreateConstants("N", "ApproveStatus"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid, "CreateBy"),
                            OOQL.CreateConstants(DateTime.Now, "CreateDate"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid, "ModifiedBy"),
                            OOQL.CreateConstants(DateTime.Now, "ModifiedDate"),
                            Formulas.Cast(OOQL.CreateConstants(guidDefault), GeneralDBType.Guid, "LastModifiedBy"),
                            OOQL.CreateConstants(DateTime.Now, "LastModifiedDate"))
                    .From("PURCHASE_ARRIVAL", "pa")
                    .InnerJoin("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "pad")
                    .On((OOQL.CreateProperty("pad.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("pa.PURCHASE_ARRIVAL_ID")))
                    .InnerJoin("ITEM", "i")
                    .On((OOQL.CreateProperty("i.ITEM_ID") == OOQL.CreateProperty("pad.ITEM_ID")))
                    .LeftJoin("PLANT", "p")
                    .On((OOQL.CreateProperty("p.PLANT_ID") == OOQL.CreateProperty("pa.RECEIVE_Owner_Org.ROid")))
                    .LeftJoin("ITEM_PLANT", "ip")
                    .On((OOQL.CreateProperty("ip.ITEM_ID") == OOQL.CreateProperty("pad.ITEM_ID"))
                        & (OOQL.CreateProperty("ip.Owner_Org.ROid") == OOQL.CreateProperty("pa.RECEIVE_Owner_Org.ROid")))
                    .LeftJoin("ITEM_STRI_DEGREE", "isd")
                    .On((OOQL.CreateProperty("isd.Owner_Org.ROid") == OOQL.CreateProperty("pa.RECEIVE_Owner_Org.ROid"))
                        & (OOQL.CreateProperty("isd.SOURCE_ID.ROid") == OOQL.CreateProperty("pa.SUPPLIER_ID")))
                    .LeftJoin("ITEM_STRI_DEGREE.ITEM_STRI_DEGREE_D", "isdd")
                    .On((OOQL.CreateProperty("isdd.ITEM_STRI_DEGREE_ID") ==
                         OOQL.CreateProperty("isd.ITEM_STRI_DEGREE_ID"))
                        & (OOQL.CreateProperty("isdd.ITEM_ID") == OOQL.CreateProperty("pad.ITEM_ID"))
                        & (OOQL.CreateProperty("isdd.ITEM_FEATURE_ID") == OOQL.CreateProperty("pad.ITEM_FEATURE_ID")))
                    .Where((OOQL.AuthFilter("PURCHASE_ARRIVAL", "pa"))
                           & (OOQL.CreateProperty("pa.DOC_NO") ==OOQL.CreateConstants(receiptNo))
                           & (OOQL.CreateProperty("pad.SequenceNumber").In(OOQL.CreateDyncParameter("SequenceNumber", paiSequenceNumber))));
            return GetService<IQueryService>().Execute(node);
        }

        /// <summary>
        /// 获取单据性质
        /// </summary>
        /// <param name="receiptNo">到货单单号</param>
        /// <param name="category">单据性质</param>
        /// <returns></returns>
        private object GetDocId(string receiptNo, string category) {
            QueryNode node =
                OOQL.Select(1,
                            OOQL.CreateProperty("pdf.DOC_ID"))
                        .From("PARA_DOC_FIL", "pdf")
                        .InnerJoin(
                            OOQL.Select(
                                OOQL.CreateProperty("pa.RECEIVE_Owner_Org.ROid", "RECEIVE_Owner_Org_ROid"),
                                OOQL.CreateProperty("pa.DOC_ID", "DOC_ID"))
                                .From("PURCHASE_ARRIVAL", "pa")
                                .Where((OOQL.CreateProperty("pa.DOC_NO") ==OOQL.CreateConstants(receiptNo))), "pa")
                        .On((OOQL.CreateProperty("pa.RECEIVE_Owner_Org_ROid") == OOQL.CreateProperty("pdf.Owner_Org.ROid")))
                        .Where((OOQL.AuthFilter("PARA_DOC_FIL", "pdf"))
                               & (OOQL.CreateProperty("pdf.CATEGORY") == OOQL.CreateConstants(category))
                               & ((OOQL.CreateProperty("pdf.SOURCE_DOC_ID") == OOQL.CreateProperty("pa.DOC_ID"))
                                  |(OOQL.CreateProperty("pdf.SOURCE_DOC_ID") ==OOQL.CreateConstants(Maths.GuidDefaultValue()))))
                        .OrderBy(OOQL.CreateOrderByItem(OOQL.CreateProperty("pdf.SOURCE_DOC_ID"), SortType.Desc));
            return GetService<IQueryService>().ExecuteScalar(node);
        }

        /// <summary>
        /// 查询到货单信息
        /// </summary>
        /// <param name="receiptNo"></param>
        /// <returns></returns>
        private DependencyObjectCollection GetPurchaseArrival(string receiptNo){
            QueryNode node =
                OOQL.Select(
                            OOQL.CreateProperty("pa.RECEIVE_Owner_Org.ROid", "RECEIVE_Owner_Org_ROid"),
                            OOQL.CreateProperty("pad.PURCHASE_ARRIVAL_D_ID", "PURCHASE_ARRIVAL_D_ID"),
                            OOQL.CreateProperty("pad.DEDUCT_ARRIVED_QTY", "DEDUCT_ARRIVED_QTY"),

                            OOQL.CreateProperty("pad.PURCHASE_TYPE", "PURCHASE_TYPE"),
                            OOQL.CreateProperty("mrd.INSPECT_MODE", "INSPECT_MODE_MRD"),
                            OOQL.CreateProperty("ip.INSPECT_MODE", "INSPECT_MODE_IP"),
                            OOQL.CreateProperty("pa.DOC_NO", "DOC_NO"),
                            OOQL.CreateProperty("pad.SequenceNumber", "SequenceNumber"))
                        .From("PURCHASE_ARRIVAL", "pa")
                        .InnerJoin("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "pad")
                        .On((OOQL.CreateProperty("pad.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("pa.PURCHASE_ARRIVAL_ID")))
                        .LeftJoin("ITEM_PLANT", "ip")
                        .On((OOQL.CreateProperty("ip.ITEM_ID") == OOQL.CreateProperty("pad.ITEM_ID"))
                            & (OOQL.CreateProperty("ip.Owner_Org.ROid") == OOQL.CreateProperty("pa.RECEIVE_Owner_Org.ROid")))
                        .LeftJoin("MO_ROUTING.MO_ROUTING_D", "mrd")
                        .On((OOQL.CreateProperty("mrd.MO_ROUTING_D_ID") ==OOQL.CreateProperty("pad.REFERENCE_SOURCE_ID.ROid")))
                        .Where((OOQL.CreateProperty("pa.DOC_NO") ==OOQL.CreateConstants(receiptNo)));
            return GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 创建结构-采购到货检验单计数单身
        /// </summary>
        /// <returns></returns>
        private DataTable CreatePoArrivalInspectionDInfo(){
            DataTable dt = new DataTable("PO_ARRIVAL_INSPECTION.PO_ARRIVAL_INSPECTION_D");
            dt.Columns.Add(new DataColumn("PO_ARRIVAL_INSPECTION_D_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("ParentId", typeof(Guid)));
            dt.Columns.Add(new DataColumn("SEQUENCE", typeof(Int32)));
            dt.Columns.Add(new DataColumn("DEFECT_CLASS", typeof(String)));
            dt.Columns.Add(new DataColumn("INSPECTION_QTY", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("INSPECTION_QQ", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("INSPECTION_ITEM_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("INSPECTION_AC", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("INSPECTION_RE", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("DECISION", typeof(Boolean)));
            dt.Columns.Add(new DataColumn("IMPACT_RESULT", typeof(Boolean)));
            dt.Columns.Add(new DataColumn("REMARK", typeof(String)));
            dt.Columns.Add(new DataColumn("DEFECTIVE_REASONS_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("CreateBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("CreateDate", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("ModifiedBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("ModifiedDate", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("LastModifiedBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("LastModifiedDate", typeof(DateTime)));
            return dt;
        }

        /// <summary>
        /// 创建结构-采购到货检验单计量单身
        /// </summary>
        /// <returns></returns>
        private DataTable CreatePoArrivalInspectionD1Info() {
            DataTable dt = new DataTable("PO_ARRIVAL_INSPECTION.PO_ARRIVAL_INSPECTION_D1");
            dt.Columns.Add(new DataColumn("PO_ARRIVAL_INSPECTION_D1_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("ParentId", typeof(Guid)));
            dt.Columns.Add(new DataColumn("SEQUENCE", typeof(int)));
            dt.Columns.Add(new DataColumn("DEFECT_CLASS", typeof(Guid)));
            dt.Columns.Add(new DataColumn("INSPECTION_QTY", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("INSPECTION_QQ", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("DECISION", typeof(Boolean)));
            dt.Columns.Add(new DataColumn("IMPACT_RESULT", typeof(Boolean)));
            dt.Columns.Add(new DataColumn("REMARK", typeof(Boolean)));
            dt.Columns.Add(new DataColumn("INSPECTION_ITEM_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("DEFECTIVE_REASONS_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("ACCEPTANCE_CONSTANT", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("SS", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("XX", typeof(decimal)));
            dt.Columns.Add(new DataColumn("CreateBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("CreateDate", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("ModifiedBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("ModifiedDate", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("LastModifiedBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("LastModifiedDate", typeof(DateTime)));
            return dt;
        }

        /// <summary>
        /// 创建结构-计数品质检验不良原因
        /// </summary>
        /// <returns></returns>
        private DataTable CreateDefectiveReasonsAiIdInfo(){
            DataTable dt = new DataTable("PO_ARRIVAL_INSPECTION.PO_ARRIVAL_INSPECTION_D.DEFECTIVE_REASONS_AI");
            dt.Columns.Add(new DataColumn("DEFECTIVE_REASONS_AI_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("ParentId", typeof(Guid)));
            dt.Columns.Add(new DataColumn("DEFECTS", typeof(Decimal)));
            dt.Columns.Add(new DataColumn("REMARK", typeof(String)));
            dt.Columns.Add(new DataColumn("DEFECTIVE_REASONS_ID", typeof(Guid)));
            dt.Columns.Add(new DataColumn("CreateBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("CreateDate", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("ModifiedBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("ModifiedDate", typeof(DateTime)));
            dt.Columns.Add(new DataColumn("LastModifiedBy", typeof(Guid)));
            dt.Columns.Add(new DataColumn("LastModifiedDate", typeof(DateTime)));
            return dt;
        }

        #endregion
    }
}
