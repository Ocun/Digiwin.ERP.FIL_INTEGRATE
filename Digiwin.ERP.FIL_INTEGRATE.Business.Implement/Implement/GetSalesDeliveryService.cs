//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-03</createDate>
//<description>获取销货单服务 实现</description>
//----------------------------------------------------------------
//20161226 modi by liwei1 for P001-161215001 逻辑调整
//20170302 modi by shenbao for P001-170302002 误差率统一乘100
//20170424 modi by wangyq for P001-170420001
// modi by 08628 for P001-171023001 20171101

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement
{
    [ServiceClass(typeof (IGetSalesDeliveryService))]
    [Description("获取销货单服务")]
    public class GetSalesDeliveryService : ServiceComponent, IGetSalesDeliveryService
    {
        #region 接口方法

        /// <summary>
        /// 根据传入的条码，获取相应的销货单信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="scanType">1.箱条码 2.单据条码 3.品号</param>
        /// <param name="siteNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="site_no">工厂</param>
        /// <returns></returns>
        public DependencyObjectCollection GetSalesDelivery(string programJobNo, string status, string scanType,
            string[] docNo, string id, string siteNo)
        {
//20161226 add by liwei1 for P001-161215001
            //public DependencyObjectCollection GetSalesDelivery(string programJobNo, string status, string scanType, string docNo, string id, string siteNo) {//20161226 mark by liwei1 for P001-161215001

            #region 参数检查

            if (Maths.IsEmpty(status))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status")); //‘入参【status】未传值’
            }
            if (Maths.IsEmpty(docNo) && Maths.IsEmpty(id))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "doc_no、ID"));
                    //‘入参【doc_no、ID】未传值’//message A111201
            }
            if (Maths.IsEmpty(siteNo))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));
                    //‘‘入参【site_no】未传值’//message A111201
            }

            #endregion

            //查询销货单信息
            QueryNode queryNode = GetSalesDeliveryQueryNode(docNo, siteNo, programJobNo, status);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 获取销货单QueryNode
        /// </summary>
        /// <param name="docNo">单据编号</param>
        /// <param name="siteNo">工厂</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private QueryNode GetSalesDeliveryQueryNode(string[] docNo, string siteNo, string programJobNo, string status)
        {
//20161226 add by liwei1 for P001-161215001
            //private QueryNode GetSalesDeliveryQueryNode(string docNo, string siteNo, string programJobNo, string status) {//20161226 mark by liwei1 for P001-161215001
            return OOQL.Select(
                OOQL.CreateConstants("99", "enterprise_no"),
                OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                OOQL.CreateConstants(programJobNo, "source_operation"),
                OOQL.CreateProperty("SALES_DELIVERY.DOC_NO", "source_no"),
                OOQL.CreateConstants(programJobNo + status, "doc_type"),
                OOQL.CreateProperty("SALES_DELIVERY.DOC_DATE", "create_date"),
                OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.SequenceNumber", "seq"),
                Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Int32, "doc_line_seq"),
                Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Int32, "doc_batch_seq"),
                OOQL.CreateConstants(string.Empty, "object_no"),
                OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(string.Empty), "item_feature_no"),
                Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(string.Empty), "item_feature_name"),
                Formulas.IsNull(
                    OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                    OOQL.CreateConstants(string.Empty), "warehouse_no"),
                Formulas.IsNull(
                    OOQL.CreateProperty("BIN.BIN_CODE"),
                    OOQL.CreateConstants(string.Empty), "storage_spaces_no"),
                Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),
                    OOQL.CreateConstants(string.Empty), "lot_no"),
                OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.BUSINESS_QTY", "doc_qty"),
                OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.ISSUED_QTY", "in_out_qty"),
                Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "unit_no"),
                OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_CODE", "main_organization")
                //20161226 add by liwei1 for P001-161215001 ===begin===
                ,
                Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_NAME"), OOQL.CreateConstants(string.Empty), "item_name"),
                //品名
                Formulas.IsNull(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty),
                    "item_spec"), //规格
                Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]
                {
                    new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N")
                        , OOQL.CreateConstants("2"))
                }), OOQL.CreateConstants(string.Empty), "lot_control_type"), //批号管控方式
                OOQL.CreateArithmetic(
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_SALES.GENERAL_DEL_OVERRUN_RATE"), OOQL.CreateConstants(0))
                    , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"),
                //允许误差率//20170302 modi by shenbao for P001-170302002 误差率统一乘100
                Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]
                {
                    OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_ID")
                    , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                    , OOQL.CreateProperty("SALES_DELIVERY_D.BUSINESS_UNIT_ID")
                    , OOQL.CreateConstants(1)
                }), //单位转换率分母
                Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]
                {
                    OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_ID")
                    , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                    , OOQL.CreateProperty("SALES_DELIVERY_D.BUSINESS_UNIT_ID")
                    , OOQL.CreateConstants(0)
                }), //单位转换率分子
                Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty),
                    "inventory_unit"), //库存单位
                //20161226 add by liwei1 for P001-161215001 ===end===
                OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"), //20170424 add by wangyq for P001-170420001
                OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"),
                //20170424 add by wangyq for P001-170420001
                // add by 08628 for P001-171023001 b
                    Formulas.Case(null,
                        Formulas.Case(null, Formulas.Case(null, OOQL.CreateConstants("", GeneralDBType.String),
                            OOQL.CreateCaseArray(
                                OOQL.CreateCaseItem(
                                    OOQL.CreateProperty("REG_G.FIFO_TYPE").IsNotNull(),
                                    OOQL.CreateProperty("REG_G.FIFO_TYPE")))),
                            OOQL.CreateCaseArray(
                                OOQL.CreateCaseItem(
                                    OOQL.CreateProperty("REG_I.FIFO_TYPE").IsNotNull(),
                                    OOQL.CreateProperty("REG_I.FIFO_TYPE")))),
                        OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                OOQL.CreateProperty("REG_I_F.FIFO_TYPE").IsNotNull(),
                                OOQL.CreateProperty("REG_I_F.FIFO_TYPE"))), "first_in_first_out_control"),
                    OOQL.CreateConstants(string.Empty, "main_warehouse_no"),
                    OOQL.CreateConstants(string.Empty, "main_storage_no")
                // add by 08628 for P001-171023001 e
                )
                .From("SALES_DELIVERY", "SALES_DELIVERY")
                .InnerJoin("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D")
                .On((OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.SALES_DELIVERY_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_ID")))
                .InnerJoin("SALES_CENTER", "SALES_CENTER")
                .On((OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.Owner_Org.ROid")))
                .InnerJoin("PLANT", "PLANT")
                .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("SALES_DELIVERY.PLANT_ID")))
                .InnerJoin("ITEM", "ITEM")
                .On((OOQL.CreateProperty("ITEM.ITEM_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.ITEM_ID")))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On((OOQL.CreateProperty("ITEM.ITEM_FEATURE.ITEM_FEATURE_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.ITEM_FEATURE_ID")))
                .LeftJoin("WAREHOUSE", "WAREHOUSE")
                .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.WAREHOUSE_ID")))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On((OOQL.CreateProperty("WAREHOUSE.BIN.BIN_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.BIN_ID")))
                .LeftJoin("ITEM_LOT", "ITEM_LOT")
                .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.ITEM_LOT_ID")))
                .LeftJoin("UNIT", "UNIT")
                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("SALES_DELIVERY_D.BUSINESS_UNIT_ID")))
                //20161226 add by liwei1 for P001-161215001 ===begin===
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    & OOQL.CreateProperty("SALES_DELIVERY.PLANT_ID") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .LeftJoin("ITEM_SALES", "ITEM_SALES")
                .On(OOQL.CreateProperty("ITEM_SALES.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")
                    &
                    OOQL.CreateProperty("SALES_DELIVERY.Owner_Org.ROid") ==
                    OOQL.CreateProperty("ITEM_SALES.Owner_Org.ROid"))
                .LeftJoin("UNIT", "STOCK_UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                //20161226 add by liwei1 for  P001-161215001 ===end===
                // add by 08628 for P001-171023001 b
                .LeftJoin(OOQL.Select(OOQL.CreateProperty("ITEM_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE_ID"),
                    OOQL.CreateProperty("FIFO_TYPE"),
                    Formulas.RowNumber("SEQ", new OverClause(new[]
                    {
                        OOQL.CreateProperty("ITEM_ID"),
                        OOQL.CreateProperty("ITEM_FEATURE_ID")
                    }, new[]
                    {
                        OOQL.CreateOrderByItem(Formulas.Case(null, OOQL.CreateProperty("CreateDate"),
                            OOQL.CreateCaseArray(
                                OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("MAIN") == OOQL.CreateConstants(1)),
                                    Formulas.Cast(OOQL.CreateConstants("9998-12-31", GeneralDBType.String),
                                        GeneralDBType.Date)))), SortType.Desc)
                    })))
                    .From("ITEM_BC_REG")
                    .Where(OOQL.CreateProperty("ITEM_FEATURE_ID").IsNotNull()
                           &
                           (OOQL.CreateProperty("ITEM_FEATURE_ID") !=
                            OOQL.CreateConstants(Maths.GuidDefaultValue()))),
                    "REG_I_F")
                .On((OOQL.CreateProperty("REG_I_F.ITEM_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.ITEM_ID"))
                    &
                    (OOQL.CreateProperty("REG_I_F.ITEM_FEATURE_ID") ==
                     OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.ITEM_FEATURE_ID"))
                    & (OOQL.CreateProperty("REG_I_F.SEQ") == OOQL.CreateConstants(1, GeneralDBType.Int32)))
                .LeftJoin(OOQL.Select(OOQL.CreateProperty("ITEM_ID"),
                    OOQL.CreateProperty("FIFO_TYPE"),
                    Formulas.RowNumber("SEQ", new OverClause(new[]
                    {
                        OOQL.CreateProperty("ITEM_ID")
                    }
                        , new[]
                        {
                            OOQL.CreateOrderByItem(Formulas.Case(null, OOQL.CreateProperty("CreateDate"),
                                OOQL.CreateCaseArray(
                                    OOQL.CreateCaseItem(
                                        (OOQL.CreateProperty("MAIN") == OOQL.CreateConstants(1, GeneralDBType.Int32)),
                                        Formulas.Cast(OOQL.CreateConstants("9998-12-31", GeneralDBType.String),
                                            GeneralDBType.Date)))), SortType.Desc)
                        })))
                    .From("ITEM_BC_REG"), "REG_I")
                .On((OOQL.CreateProperty("REG_I.ITEM_ID") == OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_D.ITEM_ID"))
                    & (OOQL.CreateProperty("REG_I.SEQ") == OOQL.CreateConstants(1, GeneralDBType.Int32)))
                .LeftJoin(OOQL.Select(OOQL.CreateProperty("FEATURE_GROUP_ID"),
                    OOQL.CreateProperty("FIFO_TYPE"),
                    Formulas.RowNumber("SEQ", new OverClause(new[]
                    {
                        OOQL.CreateProperty("FEATURE_GROUP_ID"),
                    }, new[]
                    {
                        OOQL.CreateOrderByItem(Formulas.Case(null, OOQL.CreateProperty("CreateDate"),
                            OOQL.CreateCaseArray(
                                OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("MAIN") == OOQL.CreateConstants(1, GeneralDBType.Int32)),
                                    Formulas.Cast(OOQL.CreateConstants("9998-12-31", GeneralDBType.String),
                                        GeneralDBType.Date)))), SortType.Desc)
                    })))
                    .From("ITEM_BC_REG"), "REG_G")
                .On((OOQL.CreateProperty("REG_G.FEATURE_GROUP_ID") == OOQL.CreateProperty("ITEM.FEATURE_GROUP_ID"))
                    & (OOQL.CreateProperty("REG_G.SEQ") == OOQL.CreateConstants(1, GeneralDBType.Int32)))
               
                // add by 08628 for P001-171023001 e
                .Where((OOQL.AuthFilter("SALES_DELIVERY", "SALES_DELIVERY"))
                       & (OOQL.CreateProperty("SALES_DELIVERY.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo)))
                    //20161226 add by liwei1 for P001-161215001
                    //& (OOQL.CreateProperty("SALES_DELIVERY.DOC_NO") == OOQL.CreateConstants(docNo))//20161226 mark by liwei1 for P001-161215001
                       & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo))
                       & (OOQL.CreateProperty("SALES_DELIVERY.ApproveStatus") == OOQL.CreateConstants("Y"))
                       & (OOQL.CreateProperty("SALES_DELIVERY.CATEGORY") == OOQL.CreateConstants("24")));
        }

        #endregion
    }
}