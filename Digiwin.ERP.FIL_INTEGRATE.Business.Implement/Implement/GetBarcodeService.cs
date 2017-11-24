//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-11</createDate>
//<description>获取条码服务实现</description>
//----------------------------------------------------------------
//20161205 modi by liwei1 for P001-161101003
//20161207 modi by liwei1 for B001-161206022
//20161207 modi by liwei1 for B001-161207021  设置转换为长度20位，保留8位小数位
//20161222 modi by liwei1 for P001-161215001
using System.Collections.Generic;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.Common.Services;
using System.Linq;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取条码服务
    /// </summary>
    [ServiceClass(typeof(IGetBarcodeService))]
    [Description("获取条码服务")]
    public class GetBarcodeService : ServiceComponent, IGetBarcodeService {
        

        #region IGetBarcodeService 成员
        /// <summary>
        /// 获取条码服务
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="scanType">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="status">执行动作</param>
        /// <returns></returns>
        public DependencyObjectCollection GetBarcode(string[] docNo, string scanType, List<object> itemId, List<object> itemFeatureId, string siteNo, string programJobNo, string status) {//20161222 add by liwei1 for P001-161215001
        //public DependencyObjectCollection GetBarcode(string docNo, string scanType, List<object> itemId, List<object> itemFeatureId, string siteNo, string programJobNo, string status) {//20161222 mark by liwei1 for P001-161215001
            QueryNode node = null;
            DependencyObject paraFilEntity = null;
            //存在Typekey：FIL参数(PARA_FIL)
            if (GetExistenceParaFil()) {
                DependencyObjectCollection paraFilCollection = GetParaFil();
                if (paraFilCollection.Count > 0) {
                    paraFilEntity = paraFilCollection[0];//原则上只有一笔数据，如果存在多比取第一笔
                }
            }

            if (programJobNo == "1" || programJobNo == "3") {//1.采购收货 OR 3.收货入库
                //扫描类型(scan_type)=1.箱条码
                if (scanType == "1") {
                    node = QueryPurchaseOrder(docNo, siteNo, programJobNo);
                } else if (scanType == "2") {//扫描类型(scan_type)=2 单据条码
                    node = QueryPurchaseOrder(docNo, siteNo, programJobNo, itemId, itemFeatureId, paraFilEntity);
                }  
            } else if (programJobNo == "2") {//2.采购入库
                node = QueryPurchaseReceipt(docNo, siteNo, programJobNo, itemId, itemFeatureId);
            } else if (programJobNo == "5") {//5.销售出货 
                node = QuerySalesShipment(docNo, siteNo, programJobNo, itemId, itemFeatureId, paraFilEntity);
            } else if (programJobNo == "7" && status == "A") {//7.工单发料 AND 【入参status】 =A.新增
                node = QueryMoStoreIssueA(docNo, siteNo, programJobNo, itemId, itemFeatureId, paraFilEntity);
            } else if (programJobNo == "7" && status == "S") {//7.工单发料  AND 【入参status】 =S.过账
                node = QueryMoStoreIssueS(docNo, siteNo, programJobNo, itemId, itemFeatureId, paraFilEntity);
            } else if (programJobNo == "8" && status == "A") {//8.工单退料 AND 【入参status】 =A.新增
                node = QueryMoMaterialReturnA(docNo, siteNo, programJobNo, itemId, itemFeatureId);
            } else if (programJobNo == "8" && status == "S") {//8.工单退料 AND 【入参status】 =S.过账
                node = QueryMoMaterialReturnS(docNo, siteNo, programJobNo, itemId, itemFeatureId);
            } else if (programJobNo == "9") {//9.完工入库
                node = QueryWipCompletion(docNo, siteNo, programJobNo, itemId, itemFeatureId);
            } else if (programJobNo == "11") {//11.杂项发料
                node = QueryMiscellaneousIssues(docNo, siteNo, programJobNo, itemId, itemFeatureId, paraFilEntity);
            } else if (programJobNo == "12") {//12.杂项收料
                node = QueryMiscellaneousCharge(docNo, siteNo, programJobNo, itemId, itemFeatureId, paraFilEntity);
            } else if (programJobNo == "13-1" || programJobNo == "13-2") {//13-1.调拨单 OR 13-2.调出单
                node = QueryTransferRequisition(docNo, siteNo, programJobNo, itemId, itemFeatureId, paraFilEntity);
            } else if (programJobNo == "13-3") {//13-3.拨入单 
                node = QueryDialInOrder(docNo, siteNo, programJobNo, itemId, itemFeatureId, paraFilEntity);
            }
            return this.GetService<IQueryService>().ExecuteDependencyObject(node);

        }
        #endregion

        #region 业务方法
        /// <summary>
        /// 是否存在【PARA_FIL】TypeKey
        /// </summary>
        /// <returns></returns>
        private bool GetExistenceParaFil() {
            ICreateService createSrv = this.GetService<ICreateService>("PARA_FIL");
            ITypeKeyMetadataContainer typeKeyMetadataCtr = this.GetService<ITypeKeyMetadataContainer>();
            TypeKeyMetadataCollection typekeyCollection = typeKeyMetadataCtr.TypeKeys;
            bool isOK = false;
            foreach (var item in typekeyCollection) {
                //判断结合中TypeKey是否存在PARA_FIL
                if (item.TypeKey == "PARA_FIL") {
                    isOK = true;
                    break;//20161207 add by liwei1 for B001-161206022
                }
            }
            return isOK;
        }
        #endregion

        #region 数据库相关
        private DependencyObjectCollection GetParaFil() {
            QueryNode node = OOQL.Select(
                                                        OOQL.CreateProperty("PARA_FIL.BC_INVENTORY_MANAGEMENT"),
                                                        OOQL.CreateProperty("PARA_FIL.GENERATE_BARCODE_BY_DOC"))
                                                    .From("PARA_FIL", "PARA_FIL")
                                                    .Where(OOQL.AuthFilter("PARA_FIL", "PARA_FIL"));
            return this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 根据1.箱条码查询单据
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private QueryNode QueryPurchaseOrder(string[] docNo, string siteNo, string programJobNo) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryPurchaseOrder(string docNo, string siteNo, string programJobNo) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(0, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                                                    ,20,8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                                                    ))), "barcode_qty"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"),
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "source_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber"),
                                            OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber"),
                                            OOQL.CreateConstants(0, GeneralDBType.Int32), "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                                .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")//20161205 modi by liwei1 for P001-161101003 old:InnerJoin
                                .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID")))
                                .LeftJoin("PURCHASE_ORDER", "PURCHASE_ORDER")//20161205 modi by liwei1 for P001-161101003 old:InnerJoin
                                .On((OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & ((OOQL.CreateProperty("BC_RECORD.BARCODE_NO").In(OOQL.CreateDyncParameter("BARCODE_NO", docNo)))//20161222 add by liwei1 for P001-161215001
                                    //& ((OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(docNo))//20161222 mark by liwei1 for P001-161215001
                                    & (OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")
                                            .In(OOQL.Select(
                                                    OOQL.CreateProperty("A.PLANT_ID"))
                                                .From("PLANT", "A")
                                                .Where((OOQL.CreateProperty("A.PLANT_CODE") == OOQL.CreateConstants(siteNo)))))));
        }

        /// <summary>
        /// 根据2.单据条码查询单据
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryPurchaseOrder(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 ass by liwei1 for P001-161215001
        //private QueryNode QueryPurchaseOrder(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(str, GeneralDBType.String),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(0, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                            ,20,8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                            ))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            if (paraFilEntity != null && paraFilEntity["GENERATE_BARCODE_BY_DOC"].ToBoolean()) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber"),
                        OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber"),
                        OOQL.CreateConstants(0, GeneralDBType.Int32), "source_line_seq"));
            } else {
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            }
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"));

            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                                    .From("BC_RECORD", "BC_RECORD")
                                                    .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                                    .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                                    .LeftJoin(
                                                        OOQL.Select(
                                                                OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                                                OOQL.CreateProperty("A.BC_RECORD_ID"))
                                                            .From("BC_RECORD.BC_RECORD_D", "A")
                                                            .LeftJoin("BC_PROPERTY", "B")
                                                            .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                                            .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                                    .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                                    .LeftJoin(
                                                        OOQL.Select(
                                                                OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                                                OOQL.CreateProperty("A.BC_RECORD_ID"))
                                                            .From("BC_RECORD.BC_RECORD_D", "A")
                                                            .LeftJoin("BC_PROPERTY", "B")
                                                            .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                                            .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                                    .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                                    .InnerJoin("ITEM", "ITEM")
                                                    .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                                    .LeftJoin("UNIT", "UNIT")
                                                    .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["GENERATE_BARCODE_BY_DOC"].ToBoolean()) {
                return joinOnNode.LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                    .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")//20161205 modi by liwei1 for P001-161101003 old:InnerJoin
                                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID")))
                                    .LeftJoin("PURCHASE_ORDER", "PURCHASE_ORDER")//20161205 modi by liwei1 for P001-161101003 old:InnerJoin
                                    .On((OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                                    .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                            & ((OOQL.CreateProperty("BC_RECORD.SOURCE_ID.ROid").In(OOQL.Select(
                                                    OOQL.CreateProperty("A.PURCHASE_ORDER_ID"))
                                                .From("PURCHASE_ORDER", "A")
                                                .Where((OOQL.CreateProperty("A.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo))))))//20161222 mark by liwei1 for P001-161215001
                                                //.Where((OOQL.CreateProperty("A.DOC_NO") == OOQL.CreateConstants(docNo)))))//20161222 mark by liwei1 for P001-161215001
                                            & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(OOQL.CreateDyncParameter("ITEM_ID1", itemId)))
                                            & (OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId)))
                                            & (OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")
                                                    .In(OOQL.Select(
                                                            OOQL.CreateProperty("A.PLANT_ID"))
                                                        .From("PLANT", "A")
                                                        .Where((OOQL.CreateProperty("A.PLANT_CODE") == OOQL.CreateConstants(siteNo)))))));
            } else {
                return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                            & ((OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(OOQL.CreateDyncParameter("ITEM_ID1", itemId)))
                                            & (OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId)))
                                            & (OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid")
                                                    .In(OOQL.Select(
                                                            OOQL.CreateProperty("A.PLANT_ID"))
                                                        .From("PLANT", "A")
                                                        .Where((OOQL.CreateProperty("A.PLANT_CODE") == OOQL.CreateConstants(siteNo)))))));
            }

        }

        /// <summary>
        /// 2.采购入库
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryPurchaseReceipt(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryPurchaseReceipt(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(0, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                                                    , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                            ))), "barcode_qty"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"),
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "source_no"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                            & OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(OOQL.CreateDyncParameter("ITEM_ID", itemId))
                                            & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(OOQL.CreateDyncParameter("ITEM_FEATURE_ID", itemFeatureId)));
        }

        /// <summary>
        /// 5.销售出货
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QuerySalesShipment(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QuerySalesShipment(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                propertyList.Add(Formulas.IsNull(
                         OOQL.CreateProperty("BIN.BIN_CODE"),
                         OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                propertyList.Add(OOQL.CreateProperty("BC_INVENTORY.QTY", "inventory_qty"));
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
            } else {
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "lot_no"));
            }
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                                OOQL.CreateConstants(0, GeneralDBType.Decimal),
                                OOQL.CreateCaseArray(
                                        OOQL.CreateCaseItem(
                                                (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                Formulas.Cast(
                                                        OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                                        , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                                        ))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"));
            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")));

            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                joinOnNode = joinOnNode.InnerJoin("BC_INVENTORY", "BC_INVENTORY")
                                .On((OOQL.CreateProperty("BC_RECORD.ITEM_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_ID"))
                                    & (OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_FEATURE_ID"))
                                    & (OOQL.CreateProperty("BC_INVENTORY.QTY") > OOQL.CreateConstants(0, GeneralDBType.Int32))
                                    & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO")))
                                .LeftJoin("WAREHOUSE", "WAREHOUSE")
                                .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID")))
                                .LeftJoin("WAREHOUSE.BIN", "BIN")
                                .On((OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("BC_INVENTORY.BIN_ID")))
                                .LeftJoin("ITEM_LOT", "ITEM_LOT")
                                .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID")));
            }
            //组合条件
            return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                        OOQL.CreateDyncParameter("ITEM_ID", itemId))
                                    & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                        OOQL.CreateDyncParameter("ITEM_FEATURE_ID", itemFeatureId))));
        }

        /// <summary>
        /// 7.工单发料 AND 【入参status】 =A.新增
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryMoStoreIssueA(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryMoStoreIssueA(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                propertyList.Add(Formulas.IsNull(
                         OOQL.CreateProperty("BIN.BIN_CODE"),
                         OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                propertyList.Add(OOQL.CreateProperty("BC_INVENTORY.QTY", "inventory_qty"));
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
            } else {
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "lot_no"));
            }
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(0, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                            , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                            ))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"));

            //组合关联表
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")));

            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                joinOnNode = joinOnNode.InnerJoin("BC_INVENTORY", "BC_INVENTORY")
                                    .On((OOQL.CreateProperty("BC_RECORD.ITEM_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_ID"))
                                        & (OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_FEATURE_ID"))
                                        & (OOQL.CreateProperty("BC_INVENTORY.QTY") > OOQL.CreateConstants(0, GeneralDBType.Int32))
                                        & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO")))
                                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID")))
                                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                                    .On((OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("BC_INVENTORY.BIN_ID")))
                                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                                    .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID")));
            }

            //组合关联where条件
            return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                            & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                    OOQL.CreateDyncParameter("ITEM_ID1", itemId))
                            & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                    OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId))));
        }

        /// <summary>
        /// 7.工单发料 AND 【入参status】 =S.过账
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryMoStoreIssueS(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryMoStoreIssueS(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                propertyList.Add(Formulas.IsNull(
                         OOQL.CreateProperty("BIN.BIN_CODE"),
                         OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                propertyList.Add(OOQL.CreateProperty("BC_INVENTORY.QTY", "inventory_qty"));
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
            } else {
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "lot_no"));
            }
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(0, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                            , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                            ))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"));
            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                joinOnNode = joinOnNode.InnerJoin("BC_INVENTORY", "BC_INVENTORY")
                                    .On((OOQL.CreateProperty("BC_RECORD.ITEM_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_ID"))
                                        & (OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_FEATURE_ID"))
                                        & (OOQL.CreateProperty("BC_INVENTORY.QTY") > OOQL.CreateConstants(0, GeneralDBType.Int32))
                                        & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO")))
                                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID")))
                                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                                    .On((OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("BC_INVENTORY.BIN_ID")))
                                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                                    .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID")));
            }
            //组合条件
            return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                   & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                           OOQL.CreateDyncParameter("ITEM_ID1", itemId))
                                   & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                           OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId))));
        }

        /// <summary>
        /// 8.工单退料 AND 【入参status】 =A.新增
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryMoMaterialReturnA(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryMoMaterialReturnA(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(0, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                                                    , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                                                    ))), "barcode_qty"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"),
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "source_no"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                            OOQL.CreateDyncParameter("ITEM_ID1", itemId))
                                    & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                            OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId))));
        }

        /// <summary>
        /// 8.工单退料 AND 【入参status】 =S.过账
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryMoMaterialReturnS(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryMoMaterialReturnS(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(0, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                                                    , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                                                    ))), "barcode_qty"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"),
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "source_no"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))

                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                            OOQL.CreateDyncParameter("ITEM_ID1", itemId))
                                    & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                            OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId))));
        }

        /// <summary>
        /// 9.完工入库
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryWipCompletion(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryWipCompletion(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(0, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                                                    , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                                                    ))), "barcode_qty"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"),
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("MO.DOC_NO"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "source_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("MO_PRODUCT.SequenceNumber"),
                                            OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("MO.MO_PRODUCT", "MO_PRODUCT")
                                .On((OOQL.CreateProperty("MO_PRODUCT.MO_PRODUCT_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("MO", "MO")//20161205 modi by liwei1 for P001-161101003 old:InnerJoin
                                .On((OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_PRODUCT.MO_ID")))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO").In(OOQL.CreateDyncParameter("BARCODE_NO", docNo))));//20161222 mark by liwei1 for P001-161215001
                                    //& (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(docNo, GeneralDBType.String)));//20161222 mark by liwei1 for P001-161215001
        }

        /// <summary>
        /// 11.杂项发料
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryMiscellaneousIssues(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryMiscellaneousIssues(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                propertyList.Add(Formulas.IsNull(
                         OOQL.CreateProperty("BIN.BIN_CODE"),
                         OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                propertyList.Add(OOQL.CreateProperty("BC_INVENTORY.QTY", "inventory_qty"));
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
            } else {
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "lot_no"));
            }
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(0, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                            , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                            ))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            //20161205 mrak by liwei1 for P001-161101003 ===begin===
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            //if (paraFilEntity != null && paraFilEntity["GENERATE_BARCODE_BY_DOC"].ToBoolean()) {
            //    propertyList.Add(Formulas.IsNull(
            //                OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO"),
            //                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            //    propertyList.Add(Formulas.IsNull(
            //             OOQL.CreateProperty("TRANSACTION_DOC_D.SequenceNumber"),
            //             OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            //} else {
            //20161205 mrak by liwei1 for P001-161101003 ===end===
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            //}//20161205 mrak by liwei1 for P001-161101003
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"));

            //组合关联条件
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                joinOnNode = joinOnNode.InnerJoin("BC_INVENTORY", "BC_INVENTORY")
                                    .On((OOQL.CreateProperty("BC_RECORD.ITEM_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_ID"))
                                        & (OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_FEATURE_ID"))
                                        & (OOQL.CreateProperty("BC_INVENTORY.QTY") > OOQL.CreateConstants(0, GeneralDBType.Int32))
                                        & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO")))
                                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID")))
                                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                                    .On((OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("BC_INVENTORY.BIN_ID")))
                                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                                    .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID")));
            }
            //20161205 mrak by liwei1 for P001-161101003 ===begin===
            ////必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            //if (paraFilEntity != null && paraFilEntity["GENERATE_BARCODE_BY_DOC"].ToBoolean()) {
            //    return joinOnNode.LeftJoin("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
            //                    .On((OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
            //                    .LeftJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
            //                    .On((OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID")))
            //                    .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
            //                        & ((OOQL.CreateProperty("BC_RECORD.SOURCE_ID.ROid") == OOQL.Select(
            //                                OOQL.CreateProperty("A.TRANSACTION_DOC_ID"))
            //                            .From("TRANSACTION_DOC", "A")
            //                            .Where((OOQL.CreateProperty("A.DOC_NO") == OOQL.CreateConstants(docNo, GeneralDBType.String))))
            //                        & OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
            //                                OOQL.CreateDyncParameter("ITEM_ID1", itemId))
            //                        & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
            //                                OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId))));
            //} else {
            //20161205 mrak by liwei1 for P001-161101003 ===end===
            //组合条件
            return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                            OOQL.CreateDyncParameter("ITEM_ID1", itemId))
                                    & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                            OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId))));
            //}//20161205 mrak by liwei1 for P001-161101003
        }

        /// <summary>
        /// 12.杂项收料
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryMiscellaneousCharge(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryMiscellaneousCharge(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(str, GeneralDBType.String),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(0, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                            , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                            ))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["GENERATE_BARCODE_BY_DOC"].ToBoolean()) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("TRANSACTION_DOC_D.SequenceNumber"),
                        OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            } else {
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            }
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"));

            //组合查询字段和关联表
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")));

            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["GENERATE_BARCODE_BY_DOC"].ToBoolean()) {
                return joinOnNode.LeftJoin("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                                .On((OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("TRANSACTION_DOC", "TRANSACTION_DOC")//20161205 modi by liwei1 for P001-161101003 old:InnerJoin
                                .On((OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID")))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                     & ((OOQL.CreateProperty("BC_RECORD.SOURCE_ID.ROid") .In( OOQL.Select(//20161222 mark by liwei1 for P001-161215001
                                     //& ((OOQL.CreateProperty("BC_RECORD.SOURCE_ID.ROid") == OOQL.Select(//20161222 mark by liwei1 for P001-161215001
                                             OOQL.CreateProperty("A.TRANSACTION_DOC_ID"))
                                         .From("TRANSACTION_DOC", "A")
                                         .Where((OOQL.CreateProperty("A.DOC_NO") .In( OOQL.CreateDyncParameter("DOC_NO",docNo)))))//20161222 mark by liwei1 for P001-161215001
                                         //.Where((OOQL.CreateProperty("A.DOC_NO") == OOQL.CreateConstants(docNo, GeneralDBType.String))))//20161222 mark by liwei1 for P001-161215001
                                     & OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                             OOQL.CreateDyncParameter("ITEM_ID1", itemId))
                                     & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                             OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId)))));
            } else {
                //组合条件
                return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                         & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                                 OOQL.CreateDyncParameter("ITEM_ID1", itemId))
                                         & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                                 OOQL.CreateDyncParameter("ITEM_FEATURE_ID1", itemFeatureId))));
            }
        }

        /// <summary>
        /// 13-1.调拨单 OR 13-2.调出单
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryTransferRequisition(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryTransferRequisition(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                propertyList.Add(Formulas.IsNull(
                         OOQL.CreateProperty("BIN.BIN_CODE"),
                         OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                propertyList.Add(OOQL.CreateProperty("BC_INVENTORY.QTY", "inventory_qty"));
            } else {
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"));
            }
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(str, GeneralDBType.String),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(0, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                            , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                            ))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"));

            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")));
                                //20161205 mark by liwei1 for P001-161101003 ===begin===
                                //.LeftJoin("TRANSFER_DOC.TRANSFER_DOC_D", "TRANSFER_DOC_D")
                                //.On((OOQL.CreateProperty("TRANSFER_DOC_D.TRANSFER_DOC_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_ID.ROid"))
                                //    & (OOQL.CreateProperty("TRANSFER_DOC_D.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID"))
                                //    & (OOQL.CreateProperty("TRANSFER_DOC_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                //.InnerJoin("TRANSFER_DOC", "TRANSFER_DOC")
                                //.On((OOQL.CreateProperty("TRANSFER_DOC.TRANSFER_DOC_ID") == OOQL.CreateProperty("TRANSFER_DOC_D.TRANSFER_DOC_ID")));
                                //20161205 mark by liwei1 for P001-161101003  ===begin===

            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                joinOnNode = joinOnNode.InnerJoin("BC_INVENTORY", "BC_INVENTORY")
                                    .On((OOQL.CreateProperty("BC_RECORD.ITEM_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_ID"))
                                        & (OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_FEATURE_ID"))
                                        & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO")))
                                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID")))
                                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                                    .On((OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("BC_INVENTORY.BIN_ID")))
                                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                                    .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID")));
            }
            return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & ((OOQL.CreateProperty("BC_RECORD.BARCODE_NO").In(OOQL.CreateDyncParameter("BARCODE_NO", docNo)))));//20161222 add by liwei1 for P001-161215001
                                    //& ((OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(docNo, GeneralDBType.String))));//20161222 mark by liwei1 for P001-161215001
        }

        /// <summary>
        /// 13-3.拨入单
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <returns></returns>
        private QueryNode QueryDialInOrder(string[] docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 add by liwei1 for P001-161215001
        //private QueryNode QueryDialInOrder(string docNo, string siteNo, string programJobNo, List<object> itemId, List<object> itemFeatureId, DependencyObject paraFilEntity) {//20161222 mark by liwei1 for P001-161215001
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                propertyList.Add(Formulas.IsNull(
                         OOQL.CreateProperty("BIN.BIN_CODE"),
                         OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                propertyList.Add(OOQL.CreateProperty("BC_INVENTORY.QTY", "inventory_qty"));
            } else {
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
                propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
                propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "inventory_qty"));
            }
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(str, GeneralDBType.String),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(0, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal
                                            , 20, 8//20161207 modi by liwei1 for B001-161207021 设置转换为长度20位，保留8位小数位
                                            ))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.ApproveDate", "last_transaction_date"));

            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")));
            //必须先判断paraFilEntity不等于Null，如果不存在PARA_FIL实体的话paraFilEntity为null。
            if (paraFilEntity != null && paraFilEntity["BC_INVENTORY_MANAGEMENT"].ToBoolean()) {
                joinOnNode = joinOnNode.InnerJoin("BC_INVENTORY", "BC_INVENTORY")
                                    .On((OOQL.CreateProperty("BC_RECORD.ITEM_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_ID"))
                                        & (OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_FEATURE_ID"))
                                        & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO")))
                                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID")))
                                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                                    .On((OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("BC_INVENTORY.BIN_ID")))
                                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                                    .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID")));
            }
            return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                            & (OOQL.CreateProperty("BC_RECORD.ITEM_ID").In(
                                    OOQL.CreateDyncParameter("ITEM_ID", itemId))
                            & OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID").In(
                                    OOQL.CreateDyncParameter("ITEM_FEATURE_ID", itemFeatureId))));
        }
        #endregion
    }
}
