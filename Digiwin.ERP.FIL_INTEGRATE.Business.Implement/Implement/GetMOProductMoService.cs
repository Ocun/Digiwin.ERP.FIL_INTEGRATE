//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-03</createDate>
//<description>获取入库工单服务 实现</description>
//----------------------------------------------------------------
//20161216 modi by shenbao for P001-161215001
//20170302 modi by shenbao for P001-170302002 误差率统一乘100
//20170724 modi by shenbao for P001-170717001 厂内智能物流新需求
// add by 08628 for P001-171023001 20171101
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取入库工单服务
    /// </summary>
    [ServiceClass(typeof(IGetMOProductMoService))]
    [Description("获取入库工单服务")]
    public class GetMOProductMoService : ServiceComponent, IGetMOProductMoService {
        #region 接口方法
        /// <summary>
        /// 查询工单产出信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作A.新增(查询工单信息)  S.过帐(查询领料出库单信息)</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetMOProduct(string programJobNo, string scanType, string status, string[] docNo, string id, string siteNo) {  //20170726 modi by shenbao for P001-170717001 修改docNo为数组
            #region 参数检查
            if (Maths.IsEmpty(status)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));//‘入参【status】未传值’
            }
            if (Maths.IsEmpty(id)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "ID"));//‘入参【ID】未传值’
            }
            if (Maths.IsEmpty(siteNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务

                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));//‘入参【site_no】未传值’
            }
            #endregion

            QueryNode queryNode;
            if (status == "A") {
                //查询工单产出信息
                queryNode = GetMOProductQueryNode(id, docNo, siteNo, programJobNo, status);  //20170724 modi by shenbao for P001-170717001 添加参数docno
                return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
            }
            return null;
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 获取工单产出信息查询信息
        /// </summary>
        /// <param name="id">主键</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="programJobNo"></param>
        /// <param name="status"></param>
        /// <returns></returns>
        private QueryNode GetMOProductQueryNode(string id,string[] docNo, string siteNo, string programJobNo, string status) {  //20170724 modi by shenbao for P001-170717001 添加参数docno
             string docType = programJobNo + status;
            QueryNode queryNode =
                OOQL.Select(OOQL.CreateProperty("MO_PRODUCT.PLAN_QTY", "doc_qty"),
                            Formulas.Case(null, OOQL.CreateProperty("MO_PRODUCT.COMPLETED_QTY"),new CaseItem[]{
                                new CaseItem(OOQL.CreateConstants(programJobNo)==OOQL.CreateConstants("9-1")
                                    ,OOQL.CreateProperty("MO_PRODUCT.REQ_QTY"))}, "in_out_qty"),  //20170724 modi by shenbao for P001-170717001
                            Formulas.Cast(OOQL.CreateProperty("MO_PRODUCT.SequenceNumber"), GeneralDBType.Decimal, "seq"),
                            OOQL.CreateProperty("MO.DOC_NO", "source_no"),
                            OOQL.CreateProperty("MO.DOC_DATE", "create_date"),
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_CODE"), OOQL.CreateConstants(string.Empty), "item_no"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty), "item_feature_no"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_feature_name"),
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "unit_no"),
                            OOQL.CreateConstants("99", "enterprise_no"),
                            OOQL.CreateConstants("9", "source_operation"),
                            OOQL.CreateConstants(docType, "doc_type"),
                            OOQL.CreateConstants(string.Empty, "warehouse_no"),
                            OOQL.CreateConstants(string.Empty, "storage_spaces_no"),
                            OOQL.CreateConstants(string.Empty, "lot_no"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "doc_line_seq"),
                            OOQL.CreateConstants(0, GeneralDBType.Decimal, "doc_batch_seq"),
                            OOQL.CreateConstants(string.Empty, "object_no"),
                //20161216 add by shenbao FOR P001-161215001 ===begin===
                            Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty), "item_name"),  //品名
                            Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_spec"),  //规格
                            Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                                new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL")==OOQL.CreateConstants("N")
                                    ,OOQL.CreateConstants("2"))
                            }), OOQL.CreateConstants(string.Empty), "lot_control_type"),  //批号管控方式
                            OOQL.CreateArithmetic(Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.RECEIPT_OVERRUN_RATE"),OOQL.CreateConstants(0))
                                , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"),  //允许误差率  //20170302 modi by shenbao for P001-170302002 误差率统一乘100
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]{ OOQL.CreateProperty("MO_PRODUCT.ITEM_ID")
                                , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                , OOQL.CreateProperty("MO_PRODUCT.UNIT_ID")
                                , OOQL.CreateConstants(1)}),  //单位转换率分母
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]{ OOQL.CreateProperty("MO_PRODUCT.ITEM_ID")
                                , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                , OOQL.CreateProperty("MO_PRODUCT.UNIT_ID")
                                , OOQL.CreateConstants(0)}),  //单位转换率分子
                            Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "inventory_unit") , //库存单位
                //20161216 add by shenbao FOR P001-161215001 ===end===
                // add by 08628 for P001-171023001 b
                    OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_CODE", "main_warehouse_no"),
                     OOQL.CreateProperty("MAIN_BIN.BIN_CODE", "main_storage_no"),
                   OOQL.CreateConstants(string.Empty, "first_in_first_out_control")
                // add by 08628 for P001-171023001 e
                            )
                     .From("MO.MO_PRODUCT", "MO_PRODUCT")
                     .LeftJoin("MO", "MO")
                     .On(OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_PRODUCT.MO_ID"))
                     .LeftJoin("MO.MO_D", "MO_D")
                     .On(OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_D.MO_ID"))
                     .LeftJoin("PLANT", "PLANT")
                     .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("MO.Owner_Org.ROid"))
                     .LeftJoin("ITEM", "ITEM")
                     .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("MO_PRODUCT.ITEM_ID"))
                     .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                     .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("MO_PRODUCT.ITEM_FEATURE_ID"))
                     .LeftJoin("UNIT", "UNIT")
                     .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("MO_PRODUCT.UNIT_ID"))  //20170724 modi by shenbao for P001-170717001 用工单产出信息的单位
                //20161216 add by shenbao FOR P001-161215001 ===begin===
                     .LeftJoin("ITEM_PLANT")
                     .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                        & OOQL.CreateProperty("MO.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                    .LeftJoin("UNIT", "STOCK_UNIT")
                    .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                //20161216 add by shenbao FOR P001-161215001 ===end===
                // add by 08628 for P001-171023001 b
                    .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                    .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                    .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") ==
                        OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                        & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                // add by 08628 for P001-171023001 e
                     .Where((OOQL.AuthFilter("MO.MO_PRODUCT", "MO_PRODUCT")) &
                           ((OOQL.CreateProperty("MO_PRODUCT.MO_PRODUCT_ID") == OOQL.CreateConstants(id) | (OOQL.CreateProperty("MO.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo)))) &  //20170724 modi by shenbao for P001-170717001
                            (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)) &
                            (OOQL.CreateProperty("MO.ApproveStatus") == OOQL.CreateConstants("Y")) &
                            ((OOQL.CreateConstants(programJobNo) == OOQL.CreateConstants("9-1") & OOQL.CreateProperty("MO.RECEIPT_REQ_CONTROL")==OOQL.CreateConstants(true))
                            | (OOQL.CreateConstants(programJobNo) != OOQL.CreateConstants("9-1")))));  //20170724 add by shenbao for P001-170717001
            return queryNode;
        }

        #endregion
    }
}
