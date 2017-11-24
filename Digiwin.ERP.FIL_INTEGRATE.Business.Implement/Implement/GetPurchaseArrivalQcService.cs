//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-04-18</createDate>
//<description>获取到货单质检服务</description>
//---------------------------------------------------------------- 
//20170801 add by liwei1 for P001-170717001 重构

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Utils;
using Digiwin.ERP.AI_STANDARD.Business;
using Digiwin.ERP.CUSTOM_STANDARD.Business;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取到货单质检服务
    /// </summary>
    [ServiceClass(typeof(IGetPurchaseArrivalQcService))]
    [Description("获取到货单质检服务")]
    public class GetPurchaseArrivalQcService : ServiceComponent, IGetPurchaseArrivalQcService {
        #region 接口方法
        /// <summary>
        /// 根据传入的条码，获取相应的到货单信息
        /// </summary>
        /// <param name="barcode_no">扫描单号</param>
        /// <returns></returns>
        public Hashtable GetPurchaseArrivalQc(string barcode_no) {
            try {
                // 参数检查
                if (Maths.IsEmpty(barcode_no)) {
                    var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "barcode_no"));//‘入参【barcode_no】未传值’
                }
                //组织返回结果
                Hashtable result = new Hashtable();

                //获取返回值单头与receipt_list的信息
                DependencyObjectCollection purchseArrivals = GetPurchaseArrival(barcode_no);
                if (purchseArrivals.Count>0) {
                    //返回单头信息在集合中存在都是一样的，只需要取第一笔即可
                    DependencyObject receiptData = purchseArrivals[0];
                    result.Add("delivery_no", receiptData["delivery_no"]);     //送货单
                    result.Add("supplier_no", receiptData["supplier_no"]);        //供应商
                    result.Add("supplier_name", receiptData["supplier_name"]);       //供应商名称
                    result.Add("receipt_no", receiptData["receipt_no"]);    //收货单
                    result.Add("receipt_date", receiptData["receipt_date"]);      //收货时间

                    //返回值中添加receipt_list数据
                    DependencyObjectCollection receiptList = new DependencyObjectCollection(StructureReceiptList());
                    foreach (DependencyObject item in purchseArrivals) {
                        DependencyObject newItem = receiptList.AddNew();
                        newItem["seq"] = item["seq"];
                        newItem["item_no"] = item["item_no"];
                        newItem["item_name"] = item["item_name"];
                        newItem["item_spec"] = item["item_spec"];
                        newItem["item_feature_no"] = item["item_feature_no"];
                        newItem["item_feature_name"] = item["item_feature_name"];
                        newItem["unit_no"] = item["unit_no"];
                        newItem["receipt_qty"] = item["receipt_qty"];
                        newItem["ok_qty"] = item["ok_qty"];
                        newItem["unqualified_qty"] = item["unqualified_qty"];//20170801 add by liwei1 for P001-170717001
                        newItem["checkdestroy_qty"] = item["checkdestroy_qty"];//20170801 add by liwei1 for P001-170717001
                        newItem["result_type"] = item["result_type"];
                        newItem["qc_group"] = item["qc_group"];
                        newItem["qc_degree"] = item["qc_degree"];
                        newItem["qc_level"] = item["qc_level"];
                        newItem["qc_type"] = item["qc_type"];

                        //获取qc_list数据
                        object plantId=GetPlantId(item["ITEM_ID"], item["RECEIVE_Owner_Org_ROid"], item["OPERATION_ID"]);
                        DependencyObjectCollection getQcList = GetQcList(plantId);
                        //判断是否存在数据，如果存在数据返回对应数据，如果不存在应该返回一笔空数据给APP，否则json与标准存在差异化
                        if (getQcList.Count > 0){
                            foreach (DependencyObject qcListItem in getQcList){
                                DependencyObject inspectionQty = GetInspectionQty(plantId,
                                    qcListItem["INSPECTION_ITEM_ID"],
                                    item["receipt_qty"].ToDecimal(), item["STRICTNESS_DEGREE"].ToStringExtension());

                                DependencyObject qcListEntityItem =((DependencyObjectCollection) newItem["qc_list"]).AddNew();

                                qcListEntityItem["qc_seq"] = qcListItem["qc_seq"];
                                qcListEntityItem["test_no"] = qcListItem["test_no"];
                                qcListEntityItem["test_name"] = qcListItem["test_name"];
                                qcListEntityItem["defect_level"] = qcListItem["defect_level"];
                                qcListEntityItem["reject_qty"] = qcListItem["reject_qty"];
                                if (inspectionQty != null){
                                    qcListEntityItem["test_qty"] = inspectionQty["SAMPLE_SIZE_1ST"];
                                    qcListEntityItem["acceptable_qty"] = inspectionQty["AC_1ST"];
                                    qcListEntityItem["rejected_qty"] = inspectionQty["RE_1ST"];
                                }
                                else{
                                    qcListEntityItem["test_qty"] =
                                        qcListEntityItem["acceptable_qty"] = qcListEntityItem["rejected_qty"] = 0;
                                }
                                qcListEntityItem["reason_qty"] = qcListItem["reason_qty"];
                                qcListEntityItem["return_qty"] = qcListItem["return_qty"];
                                qcListEntityItem["reason_no"] = string.Empty;
                                qcListEntityItem["measure_max"] = qcListItem["measure_max"];
                                qcListEntityItem["measure_min"] = qcListItem["measure_min"];
                                qcListEntityItem["result_type"] = qcListItem["result_type"];

                                #region 与SD确认过，返回空对象即可，无需给默认值

                                SetDefaultValue(qcListEntityItem);

                                #endregion
                            }
                        }
                        else{
                            DependencyObject qcListEntityItem = ((DependencyObjectCollection)newItem["qc_list"]).AddNew();
                            qcListEntityItem["defect_level"] =
                                qcListEntityItem["result_type"] =
                                    qcListEntityItem["test_name"] =
                                        qcListEntityItem["reason_no"] = qcListEntityItem["test_no"] = string.Empty;
                            SetDefaultValue(qcListEntityItem);
                        }
                    }
                    result.Add("receipt_list", receiptList);  
                }
                return result;
            } catch (Exception) {
                throw;
            }
        }

        

        #endregion

        #region 业务方法

        /// <summary>
        /// 构建默认返回值
        /// </summary>
        /// <param name="qcListEntityItem"></param>
        private void SetDefaultValue(DependencyObject qcListEntityItem) {
            //构建reason_list返回值
            DependencyObject reasonListEntity = ((DependencyObjectCollection)qcListEntityItem["reason_list"]).AddNew();
            reasonListEntity["reason_no"] = reasonListEntity["reason_code_name"] = string.Empty;

            //构建attrib_list返回值
            DependencyObject attribListEntity = ((DependencyObjectCollection)qcListEntityItem["attrib_list"]).AddNew();
            attribListEntity["result_type"] = string.Empty;
        }

        /// <summary>
        /// 检验单获取质检方案相关抽样数量
        /// </summary>
        /// <param name="paraInspectionPlanId">质检方案id</param>
        /// <param name="paraInspectionItemId">检验项目</param>
        /// <param name="paraQty">送检批量</param>
        /// <param name="paraDegree">宽严程度</param>
        /// <returns></returns>
        public DependencyObject GetInspectionQty(object paraInspectionPlanId, object paraInspectionItemId,
            decimal paraQty, string paraDegree){
            IQueryService querySrv = GetService<IQueryService>();
            DependencyObject queryPlan = null;
                QueryNode node = OOQL.Select((Formulas.Case(null, OOQL.CreateProperty("A.MI_AQL"),
                    OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem(
                            OOQL.CreateProperty("B.DEFECT_CLASS") == OOQL.CreateConstants("1"),
                            OOQL.CreateProperty("A.CR_AQL")),
                        OOQL.CreateCaseItem(
                            OOQL.CreateProperty("B.DEFECT_CLASS") == OOQL.CreateConstants("2"),
                            OOQL.CreateProperty("A.MA_AQL"))), "AQL")),
                    (Formulas.Case(null, OOQL.CreateProperty("A.MI_LQ"),
                        OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                OOQL.CreateProperty("B.DEFECT_CLASS") == OOQL.CreateConstants("1"),
                                OOQL.CreateProperty("A.CR_LQ")),
                            OOQL.CreateCaseItem(
                                OOQL.CreateProperty("B.DEFECT_CLASS") == OOQL.CreateConstants("2"),
                                OOQL.CreateProperty("A.MA_LQ"))), "LQ")),
                    (Formulas.Case(null, OOQL.CreateProperty("A.MI_RQL"),
                        OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                OOQL.CreateProperty("B.DEFECT_CLASS") == OOQL.CreateConstants("1"),
                                OOQL.CreateProperty("A.CR_RQL")),
                            OOQL.CreateCaseItem(
                                OOQL.CreateProperty("B.DEFECT_CLASS") == OOQL.CreateConstants("2"),
                                OOQL.CreateProperty("A.MA_RQL"))), "RQL")),
                    OOQL.CreateProperty("A.INSPECTION_TIMES", "INSPECTION_TIMES"),
                    OOQL.CreateProperty("B.IL", "IL"),
                    OOQL.CreateProperty("B.DL", "DL"),
                    OOQL.CreateProperty("A.INSPECTION_METHOD", "INSPECTION_METHOD"),
                    OOQL.CreateProperty("A.AI_STANDARD", "AI_STANDARD"),
                    OOQL.CreateProperty("A.VI_STANDARD", "VI_STANDARD"),
                    OOQL.CreateProperty("A.CUSTOM_STANDARD_ID", "CUSTOM_STANDARD_ID"),
                    OOQL.CreateProperty("B.DEFECT_CLASS", "DEFECT_CLASS"),
                    OOQL.CreateProperty("A.INSPECTION_PLAN_ID", "INSPECTION_PLAN_ID"),
                    OOQL.CreateProperty("B.INSPECTION_ITEM_ID", "INSPECTION_ITEM_ID"),
                    OOQL.CreateProperty("A.MODE", "MODE"))
                    .From("INSPECTION_PLAN", "A")
                    .LeftJoin("INSPECTION_PLAN.INSPECTION_PLAN_D", "B")
                    .On(OOQL.CreateProperty("A.INSPECTION_PLAN_ID") == OOQL.CreateProperty("B.INSPECTION_PLAN_ID"))
                    .Where(OOQL.AuthFilter("INSPECTION_PLAN", "A") &
                           OOQL.CreateProperty("A.INSPECTION_PLAN_ID") == OOQL.CreateConstants(paraInspectionPlanId)
                           &OOQL.CreateProperty("B.INSPECTION_ITEM_ID") == OOQL.CreateConstants(paraInspectionItemId));
                DependencyObjectCollection coll = querySrv.ExecuteDependencyObject(node);
                if (coll != null && coll.Count > 0){
                    queryPlan = coll[0];
                }
 
            if (queryPlan == null){
                return null;
            }
            //6.2	根据抽样标准call不同的服务
            //6.2.1	全检
            DependencyObject result;
            if (queryPlan["INSPECTION_METHOD"].ToInt32() == 2){
                return null;
            }
            INumberedSampleTestService numberedSampleTestService =GetService<INumberedSampleTestService>("AI_STANDARD");
            if (queryPlan["AI_STANDARD"].Equals("1") || queryPlan["AI_STANDARD"].Equals("4")){
                result = numberedSampleTestService.GetNumberStandard(paraQty, paraDegree, queryPlan["IL"].ToString(),
                    queryPlan["AQL"].ToString(), queryPlan["INSPECTION_TIMES"].ToString());
            }
            else if (queryPlan["AI_STANDARD"].Equals("2")){
                if (queryPlan["MODE"].Equals("1")){
                    result = numberedSampleTestService.GetNumberStandard(paraQty, queryPlan["LQ"].ToString(),
                        queryPlan["INSPECTION_TIMES"].ToString());
                }
                else{
                    result = numberedSampleTestService.GetNumberStandard(paraQty, queryPlan["LQ"].ToString(),
                        queryPlan["INSPECTION_TIMES"].ToString(), queryPlan["IL"].ToString());
                }
            }
            else if (queryPlan["AI_STANDARD"].Equals("0")){
                result = numberedSampleTestService.GetNumberStandard(queryPlan["DL"].ToString(),
                    queryPlan["RQL"].ToString());
            }
            else{
                return null; 
            }

            //6.2.4	抽检GB2829（暂不实现）

            //6.2.5	抽检自定义
            if (!Maths.IsEmpty(queryPlan["CUSTOM_STANDARD_ID"])){
                ICustomTestService customTestService = GetService<ICustomTestService>("CUSTOM_STANDARD");
                DependencyObject obj = customTestService.GetCustomStandard(queryPlan["CUSTOM_STANDARD_ID"], paraQty);
                result["SAMPLE_SIZE_1ST"] = Convert.ToInt32(obj["SAMPLE_SIZE"].ToDecimal());
                result["AC_1ST"] = Convert.ToInt32(obj["AC"].ToDecimal());
                result["RE_1ST"] = Convert.ToInt32(obj["RE"].ToDecimal());
                result["Result"] = obj["Result"].ToBoolean(); 
  
            }
            //6.2.6	回传处理，如果样本量大于送检量,则样本量等于送检数量 zhufei
            //(/  / / /)>送检量（paraQty）
            decimal sampleSize1St = result["SAMPLE_SIZE_1ST"].ToDecimal();
            decimal sampleSize2Nd = result["SAMPLE_SIZE_2ND"].ToDecimal();
            decimal sampleSize3Rd = result["SAMPLE_SIZE_3RD"].ToDecimal();
            decimal sampleSize4Th = result["SAMPLE_SIZE_4TH"].ToDecimal();
            decimal sampleSize5Th = result["SAMPLE_SIZE_5TH"].ToDecimal();
            if (result["SAMPLE_SIZE_1ST"].ToDecimal() > paraQty){
                sampleSize1St = paraQty;
            }
            if (result["SAMPLE_SIZE_2ND"].ToDecimal() > paraQty){
                sampleSize2Nd = paraQty;
            }
            if (result["SAMPLE_SIZE_3RD"].ToDecimal() > paraQty){
                sampleSize3Rd = paraQty;
            }
            if (result["SAMPLE_SIZE_4TH"].ToDecimal() > paraQty){
                sampleSize4Th = paraQty;
            }
            if (result["SAMPLE_SIZE_5TH"].ToDecimal() > paraQty){
                sampleSize5Th = paraQty;
            }
            
            DependencyObjectType root01 = new DependencyObjectType("INSPECTION_QTY");
            root01.RegisterSimpleProperty("SAMPLE_SIZE_1ST", typeof (decimal));
            root01.RegisterSimpleProperty("AC_1ST", typeof (decimal));
            root01.RegisterSimpleProperty("RE_1ST", typeof (decimal));
            root01.RegisterSimpleProperty("SAMPLE_SIZE_2ND", typeof (decimal));
            root01.RegisterSimpleProperty("AC_2ND", typeof (decimal));
            root01.RegisterSimpleProperty("RE_2ND", typeof (decimal));
            root01.RegisterSimpleProperty("SAMPLE_SIZE_3RD", typeof (decimal));
            root01.RegisterSimpleProperty("AC_3RD", typeof (decimal));
            root01.RegisterSimpleProperty("RE_3RD", typeof (decimal));
            root01.RegisterSimpleProperty("SAMPLE_SIZE_4TH", typeof (decimal));
            root01.RegisterSimpleProperty("AC_4TH", typeof (decimal));
            root01.RegisterSimpleProperty("RE_4TH", typeof (decimal));
            root01.RegisterSimpleProperty("SAMPLE_SIZE_5TH", typeof (decimal));
            root01.RegisterSimpleProperty("AC_5TH", typeof (decimal));
            root01.RegisterSimpleProperty("RE_5TH", typeof (decimal));
            root01.RegisterSimpleProperty("Result", typeof (Boolean));
            root01.RegisterSimpleProperty("Msg", typeof (String));
            DependencyObject collection = new DependencyObject(root01);
            collection["SAMPLE_SIZE_1ST"] = sampleSize1St; //            result["SAMPLE_SIZE_1ST"];
            collection["AC_1ST"] = result["AC_1ST"];
            collection["RE_1ST"] = result["RE_1ST"];
            collection["SAMPLE_SIZE_2ND"] = sampleSize2Nd; // result["SAMPLE_SIZE_2ND"];
            collection["AC_2ND"] = result["AC_2ND"];
            collection["RE_2ND"] = result["RE_2ND"];
            collection["SAMPLE_SIZE_3RD"] = sampleSize3Rd; // result["SAMPLE_SIZE_3RD"];
            collection["AC_3RD"] = result["AC_3RD"];
            collection["RE_3RD"] = result["RE_3RD"];
            collection["SAMPLE_SIZE_4TH"] = sampleSize4Th; // result["SAMPLE_SIZE_4TH"];
            collection["AC_4TH"] = result["AC_4TH"];
            collection["RE_4TH"] = result["RE_4TH"];
            collection["SAMPLE_SIZE_5TH"] = sampleSize5Th; // result["SAMPLE_SIZE_5TH"];
            collection["AC_5TH"] = result["AC_5TH"];
            collection["RE_5TH"] = result["RE_5TH"];
            collection["Result"] = result["Result"];
            collection["Msg"] = result["Msg"];
            return collection;
        }

        /// <summary>
        /// 构建reason_list返回值
        /// </summary>
        /// <returns></returns>
        private DependencyObjectType StructureReceiptList() {
            DependencyObjectType receiptListType = new DependencyObjectType("receipt_list");
            receiptListType.RegisterSimpleProperty("seq", typeof(int));
            receiptListType.RegisterSimpleProperty("item_no", typeof(string));
            receiptListType.RegisterSimpleProperty("item_name", typeof(string));
            receiptListType.RegisterSimpleProperty("item_spec", typeof(string));
            receiptListType.RegisterSimpleProperty("item_feature_no", typeof(string));
            receiptListType.RegisterSimpleProperty("item_feature_name", typeof(string));
            receiptListType.RegisterSimpleProperty("unit_no", typeof(string));
            receiptListType.RegisterSimpleProperty("receipt_qty", typeof(decimal));
            receiptListType.RegisterSimpleProperty("ok_qty", typeof(decimal));
            receiptListType.RegisterSimpleProperty("unqualified_qty", typeof(decimal));//20170801 add by liwei1 for P001-170717001
            receiptListType.RegisterSimpleProperty("checkdestroy_qty", typeof(decimal));//20170801 add by liwei1 for P001-170717001reason_no
            receiptListType.RegisterSimpleProperty("result_type", typeof(string));
            receiptListType.RegisterSimpleProperty("qc_group", typeof(string));
            receiptListType.RegisterSimpleProperty("qc_degree", typeof(string));
            receiptListType.RegisterSimpleProperty("qc_level", typeof(string));
            receiptListType.RegisterSimpleProperty("qc_type", typeof(string));
            receiptListType.RegisterCollectionProperty("qc_list", StructureQcList());
            return receiptListType;
        }

        /// <summary>
        /// 构建reason_list返回值
        /// </summary>
        /// <returns></returns>
        private DependencyObjectType StructureQcList() {
            DependencyObjectType qcListType = new DependencyObjectType("qc_list");
            qcListType.RegisterSimpleProperty("qc_seq", typeof(int));
            qcListType.RegisterSimpleProperty("test_no", typeof(string));
            qcListType.RegisterSimpleProperty("test_name", typeof(string));
            qcListType.RegisterSimpleProperty("defect_level", typeof(string));
            qcListType.RegisterSimpleProperty("reject_qty", typeof(decimal));
            qcListType.RegisterSimpleProperty("test_qty", typeof(decimal));
            qcListType.RegisterSimpleProperty("reason_qty", typeof(decimal));
            qcListType.RegisterSimpleProperty("acceptable_qty", typeof(decimal));//20170801 add by liwei1 for P001-170717001
            qcListType.RegisterSimpleProperty("rejected_qty", typeof(decimal));//20170801 add by liwei1 for P001-170717001
            qcListType.RegisterSimpleProperty("return_qty", typeof(decimal));
            qcListType.RegisterSimpleProperty("reason_no", typeof(string));      //缺点原因
            qcListType.RegisterSimpleProperty("measure_max", typeof(decimal));
            qcListType.RegisterSimpleProperty("measure_min", typeof(decimal));
            qcListType.RegisterSimpleProperty("result_type", typeof(string));
            qcListType.RegisterCollectionProperty("reason_list", StructureReasonList());
            qcListType.RegisterCollectionProperty("attrib_list", StructureAttribList());
            return qcListType;
        }

        /// <summary>
        /// 构建reason_list返回值
        /// </summary>
        /// <returns></returns>
        private DependencyObjectType StructureReasonList() {
            DependencyObjectType type = new DependencyObjectType("reason_list");
            type.RegisterSimpleProperty("reason_no", typeof(string));      //缺点原因编号
            type.RegisterSimpleProperty("reason_code_name", typeof(string));      //缺点原因
            type.RegisterSimpleProperty("defect_qty", typeof(decimal));      //缺点数量
            return type;
        }

        /// <summary>
        /// 构建attrib_list返回值
        /// </summary>
        /// <returns></returns>
        private DependencyObjectType StructureAttribList() {
            DependencyObjectType type = new DependencyObjectType("attrib_list");
            type.RegisterSimpleProperty("attrib_value", typeof(decimal));        //测量值
            type.RegisterSimpleProperty("result_type", typeof(string));     //判定状态
            return type;
        }

        /// <summary>
        /// 获取qc_list数据
        /// </summary>
        /// <param name="plantId"></param>
        /// <returns></returns>
        private DependencyObjectCollection GetQcList(object plantId) {
            QueryNode node =
                OOQL.Select(
                            OOQL.CreateProperty("INSPECTION_PLAN_D.SEQUENCE", "qc_seq"),
                            Formulas.IsNull(OOQL.CreateProperty("INSPECTION_ITEM.INSPECTION_ITEM_ID"),
                                OOQL.CreateConstants(Maths.GuidDefaultValue()), "INSPECTION_ITEM_ID"),
                            Formulas.IsNull(OOQL.CreateProperty("INSPECTION_ITEM.INSPECTION_ITEM_CODE"),
                                OOQL.CreateConstants(string.Empty), "test_no"),
                            Formulas.IsNull(OOQL.CreateProperty("INSPECTION_ITEM.INSPECTION_ITEM_NAME"),
                                OOQL.CreateConstants(string.Empty), "test_name"),
                            OOQL.CreateProperty("INSPECTION_PLAN_D.DEFECT_CLASS", "defect_level"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "reject_qty"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "test_qty"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "reason_qty"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "return_qty"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "acceptable_qty"),//20170801 add by liwei1 for P001-170717001
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "rejected_qty"),//20170801 add by liwei1 for P001-170717001
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "measure_max"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "measure_min"),
                            OOQL.CreateConstants("Y", GeneralDBType.String, "result_type"))
                        .From("INSPECTION_PLAN", "INSPECTION_PLAN")
                        .InnerJoin("INSPECTION_PLAN.INSPECTION_PLAN_D", "INSPECTION_PLAN_D")
                        .On((OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_ID") == OOQL.CreateProperty("INSPECTION_PLAN_D.INSPECTION_PLAN_ID")))
                        .LeftJoin("INSPECTION_ITEM", "INSPECTION_ITEM")
                        .On((OOQL.CreateProperty("INSPECTION_PLAN_D.INSPECTION_ITEM_ID") == OOQL.CreateProperty("INSPECTION_ITEM.INSPECTION_ITEM_ID")))
                        .Where(OOQL.AuthFilter("INSPECTION_PLAN", "INSPECTION_PLAN")
                              & (OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_ID") == OOQL.CreateConstants(plantId)));
            return GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 根据品号、工厂获取质检方案
        /// </summary>
        /// <param name="paraItemId"></param>
        /// <param name="paraPlantId"></param>
        /// <param name="paraOperationId"></param>
        /// <returns></returns>
        private object GetPlantId(object paraItemId, object paraPlantId, object paraOperationId) {
            object plantId = Maths.GuidDefaultValue();//返回值默认值
            QueryNode node = OOQL.Select(OOQL.CreateProperty("ITEM_PLANT.QC_CATEGORY_ID"))
                 .From("ITEM_PLANT", "ITEM_PLANT")
                 .Where(OOQL.AuthFilter("ITEM_PLANT", "ITEM_PLANT")
                 & OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(paraPlantId)
                 & OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateConstants(paraItemId));
            object queryB = GetService<IQueryService>().ExecuteScalar(node);

            if (Maths.IsEmpty(paraOperationId)) {//工艺为空
                node = OOQL.Select(OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_ID", "PLAN_ID"))
                                        .From("INSPECTION_PLAN", "INSPECTION_PLAN")
                                        .Where(OOQL.AuthFilter("INSPECTION_PLAN", "INSPECTION_PLAN")
                                        & OOQL.CreateProperty("INSPECTION_PLAN.Owner_Org.ROid") == OOQL.CreateConstants(paraPlantId)
                                        & (OOQL.CreateProperty("INSPECTION_PLAN.ITEM_ID") == OOQL.CreateConstants(paraItemId))
                                        & (OOQL.CreateProperty("INSPECTION_PLAN.ApproveStatus") == OOQL.CreateConstants("Y")));
                object queryA = GetService<IQueryService>().ExecuteScalar(node);
                if (queryA == null) {//工厂+ 品号
                    node = OOQL.Select(OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_ID", "PLAN_ID"))
                           .From("INSPECTION_PLAN", "INSPECTION_PLAN")
                           .Where(OOQL.AuthFilter("INSPECTION_PLAN", "INSPECTION_PLAN")
                           & OOQL.CreateProperty("INSPECTION_PLAN.Owner_Org.ROid") == OOQL.CreateConstants(paraPlantId)
                           & OOQL.CreateProperty("INSPECTION_PLAN.QC_CATEGORY_ID") == OOQL.CreateConstants(queryB)
                           & OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_TYPE") == OOQL.CreateConstants("1")
                           & (OOQL.CreateProperty("INSPECTION_PLAN.ApproveStatus") == OOQL.CreateConstants("Y")));
                    object queryC = GetService<IQueryService>().ExecuteScalar(node);
                    if (queryC != null) {//工厂+ 品管类别
                        plantId = queryC;
                    }
                } else {
                    plantId = queryA;
                }
            } else {
                if (Maths.IsEmpty(paraItemId)) {//品号为空
                    node = OOQL.Select(OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_ID", "PLAN_ID"))
                          .From("INSPECTION_PLAN", "INSPECTION_PLAN")
                          .Where(OOQL.AuthFilter("INSPECTION_PLAN", "INSPECTION_PLAN")
                          & OOQL.CreateProperty("INSPECTION_PLAN.Owner_Org.ROid") == OOQL.CreateConstants(paraPlantId)
                          & OOQL.CreateProperty("INSPECTION_PLAN.OPERATION_ID") == OOQL.CreateConstants(paraOperationId)
                          & (OOQL.CreateProperty("INSPECTION_PLAN.ApproveStatus") == OOQL.CreateConstants("Y")));
                    object queryX = GetService<IQueryService>().ExecuteScalar(node);

                    if (queryX != null) {//工厂+工艺
                        plantId = queryX;
                    }
                } else {
                    node = OOQL.Select(OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_ID", "PLAN_ID"))
                           .From("INSPECTION_PLAN", "INSPECTION_PLAN")
                           .Where(OOQL.AuthFilter("INSPECTION_PLAN", "INSPECTION_PLAN")
                           & OOQL.CreateProperty("INSPECTION_PLAN.Owner_Org.ROid") == OOQL.CreateConstants(paraPlantId)
                           & OOQL.CreateProperty("INSPECTION_PLAN.ITEM_ID") == OOQL.CreateConstants(paraItemId)
                           & OOQL.CreateProperty("INSPECTION_PLAN.OPERATION_ID") == OOQL.CreateConstants(paraOperationId)
                           & (OOQL.CreateProperty("INSPECTION_PLAN.ApproveStatus") == OOQL.CreateConstants("Y")));
                    object queryD = GetService<IQueryService>().ExecuteScalar(node);

                    if (queryD == null) {//工厂+品号+工艺
                        node = OOQL.Select(OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_ID", "PLAN_ID"))
                        .From("INSPECTION_PLAN", "INSPECTION_PLAN")
                        .Where(OOQL.AuthFilter("INSPECTION_PLAN", "INSPECTION_PLAN")
                            & OOQL.CreateProperty("INSPECTION_PLAN.Owner_Org.ROid") == OOQL.CreateConstants(paraPlantId)
                            & OOQL.CreateProperty("INSPECTION_PLAN.QC_CATEGORY_ID") == OOQL.CreateConstants(queryB)
                            & OOQL.CreateProperty("INSPECTION_PLAN.OPERATION_ID") == OOQL.CreateConstants(paraOperationId)
                            & (OOQL.CreateProperty("INSPECTION_PLAN.ApproveStatus") == OOQL.CreateConstants("Y")));
                        object queryE = GetService<IQueryService>().ExecuteScalar(node);

                        if (queryE == null) {
                            node = OOQL.Select(OOQL.CreateProperty("INSPECTION_PLAN.INSPECTION_PLAN_ID", "PLAN_ID"))
                                   .From("INSPECTION_PLAN", "INSPECTION_PLAN")
                                   .Where(OOQL.AuthFilter("INSPECTION_PLAN", "INSPECTION_PLAN")
                                        & OOQL.CreateProperty("INSPECTION_PLAN.Owner_Org.ROid") == OOQL.CreateConstants(paraPlantId)
                                        & OOQL.CreateProperty("INSPECTION_PLAN.OPERATION_ID") == OOQL.CreateConstants(paraOperationId)
                                        & OOQL.CreateProperty("INSPECTION_PLAN.ApproveStatus") == OOQL.CreateConstants("Y"));

                            object queryX = GetService<IQueryService>().ExecuteScalar(node);
                            if (queryX != null) {
                                plantId = queryX;
                            }
                        } else {
                            plantId = queryE;
                        }
                    } else {
                        plantId = queryD;
                    }
                }
            }
            return plantId;
        }

        /// <summary>
        /// 获取返回值单头与receipt_list的信息
        /// </summary>
        /// <param name="barcodeNo"></param>
        /// <returns></returns>
        private DependencyObjectCollection GetPurchaseArrival(string barcodeNo) {
            JoinOnNode joinOnNode =
                OOQL.Select(
                            Formulas.IsNull(
                                    OOQL.CreateProperty("ITEM_STRI_DEGREE_D.STRICTNESS_DEGREE"),
                                    OOQL.CreateConstants("1", GeneralDBType.String), "STRICTNESS_DEGREE"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D.ITEM_ID", "ITEM_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.RECEIVE_Owner_Org.ROid", "RECEIVE_Owner_Org_ROid"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D.OPERATION_ID", "OPERATION_ID"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D.SequenceNumber", "seq"),
                            OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                            OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                            OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                            Formulas.IsNull(
                                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_no"),
                            Formulas.IsNull(
                                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_name"),
                            Formulas.IsNull(
                                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "unit_no"),
                            OOQL.CreateArithmetic(
                                    OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUSINESS_QTY"),
                                    OOQL.CreateProperty("PURCHASE_ARRIVAL_D.INSPECTED_QTY"), ArithmeticOperators.Sub, "receipt_qty"),
                            OOQL.CreateArithmetic(
                                    OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUSINESS_QTY"),
                                    OOQL.CreateProperty("PURCHASE_ARRIVAL_D.INSPECTED_QTY"), ArithmeticOperators.Sub, "ok_qty"),
                            OOQL.CreateConstants(0m, GeneralDBType.Decimal, "unqualified_qty"),//20170801 add by liwei1 for P001-170717001
                            OOQL.CreateConstants(0m, GeneralDBType.Decimal, "checkdestroy_qty"),//20170801 add by liwei1 for P001-170717001
                            OOQL.CreateConstants(string.Empty,  "result_type"),
                            OOQL.CreateConstants(string.Empty,  "qc_group"),
                            OOQL.CreateConstants(string.Empty,  "qc_degree"),
                            OOQL.CreateConstants(string.Empty,  "qc_level"),
                            OOQL.CreateConstants(string.Empty,  "qc_type"),
                            OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO", "delivery_no"),
                            OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE", "supplier_no"),
                            OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME", "supplier_name"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO", "receipt_no"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_DATE", "receipt_date"))
                        .From("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                        .InnerJoin("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID")))
                        .InnerJoin("ITEM", "ITEM")
                        .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D.ITEM_ID")))
                        .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM.ITEM_FEATURE.ITEM_FEATURE_ID")))
                        .LeftJoin("UNIT", "UNIT")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID")))
                        .LeftJoin("FIL_ARRIVAL.FIL_ARRIVAL_D", "FIL_ARRIVAL_D")
                        .On((OOQL.CreateProperty("FIL_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID")))
                        .LeftJoin("SUPPLIER", "SUPPLIER")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.SUPPLIER_ID") == OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID")))
                        .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID"))
                            & (OOQL.CreateProperty("PURCHASE_ARRIVAL.RECEIVE_Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid")))
                        .LeftJoin("MO_ROUTING.MO_ROUTING_D", "MO_ROUTING_D")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.REFERENCE_SOURCE_ID.RTK") == OOQL.CreateConstants("MO_ROUTING.MO_ROUTING_D"))
                            & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.REFERENCE_SOURCE_ID.ROid") == OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_D_ID")))
                        .LeftJoin("ITEM_STRI_DEGREE", "ITEM_STRI_DEGREE")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.RECEIVE_Owner_Org.ROid") == OOQL.CreateProperty("ITEM_STRI_DEGREE.Owner_Org.ROid"))
                            & (OOQL.CreateProperty("PURCHASE_ARRIVAL.SUPPLIER_ID") == OOQL.CreateProperty("ITEM_STRI_DEGREE.SOURCE_ID.ROid")))
                        .LeftJoin("ITEM_STRI_DEGREE.ITEM_STRI_DEGREE_D", "ITEM_STRI_DEGREE_D")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_ID") == OOQL.CreateProperty("ITEM_STRI_DEGREE_D.ITEM_ID"))
                            & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_STRI_DEGREE_D.ITEM_FEATURE_ID"))
                            & (OOQL.CreateProperty("ITEM_STRI_DEGREE_D.ITEM_STRI_DEGREE_ID") == OOQL.CreateProperty("ITEM_STRI_DEGREE.ITEM_STRI_DEGREE_ID")));

            //共同存在的条件
            QueryConditionGroup conditionGroup = (OOQL.AuthFilter("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL"))
                                                    & (OOQL.CreateProperty("PURCHASE_ARRIVAL.CATEGORY") == OOQL.CreateConstants("36"))
                                                    & (OOQL.CreateProperty("PURCHASE_ARRIVAL.ApproveStatus") == OOQL.CreateConstants("Y"))
                                                    & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.RECEIPT_CLOSE") == OOQL.CreateConstants("0"))
                                                    & (OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO") == OOQL.CreateConstants(barcodeNo))
                                                    & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.BUSINESS_QTY") > OOQL.CreateProperty("PURCHASE_ARRIVAL_D.INSPECTED_QTY"));
            //主查询中增加不一样的条件
            QueryConditionGroup conditionGroupUnionOne = conditionGroup & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_TYPE") == OOQL.CreateConstants("3"))
                                                  & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.INSPECTION_STATUS") != OOQL.CreateConstants("1"))
                                                  & OOQL.CreateProperty("MO_ROUTING_D.INSPECT_MODE") == OOQL.CreateConstants("2");
            //第一个Union中增加不一样的条件
            QueryConditionGroup conditionGroupUnionTwo = conditionGroup & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_TYPE") != OOQL.CreateConstants("3"))
                                                  & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.INSPECTION_STATUS") != OOQL.CreateConstants("1"))
                                                  & OOQL.CreateProperty("ITEM_PLANT.INSPECT_MODE") == OOQL.CreateConstants("2")
                                                  & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.REFERENCE_SOURCE_ID.RTK") != OOQL.CreateConstants("WIP"));
            //第二个Union中增加不一样的条件
            QueryConditionGroup conditionGroupUnionThree = conditionGroup & (OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_TYPE") == OOQL.CreateConstants("3"))
                                    & ((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.INSPECTION_STATUS")) != OOQL.CreateConstants("1"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.INSPECT_MODE")) == OOQL.CreateConstants("2")
                                    & ((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.REFERENCE_SOURCE_ID.RTK")) == OOQL.CreateConstants("WIP"));
            //组合node
            QueryNode queryNode = joinOnNode.Where(conditionGroupUnionOne)
                .Union(joinOnNode.Where(conditionGroupUnionTwo), true)
                .Union(joinOnNode.Where(conditionGroupUnionThree), true);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }
        #endregion
    }
}
