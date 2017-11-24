//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2017-02-13</createDate>
//<description>获取销退单服务</description>
//---------------------------------------------------------------- 
//20170424 modi by wangyq for P001-170420001

// modi by 08628 for P001-171023001 20171101

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取销退单服务
    /// </summary>
    [ServiceClass(typeof(IGetSalesReturnService))]
    [Description("获取销退单服务")]
    public class GetSalesReturnService : ServiceComponent, IGetSalesReturnService {
        #region 相关服务

        private IInfoEncodeContainer _infoEncodeContainer;
        /// <summary>
        /// 信息编码服务
        /// </summary>
        public IInfoEncodeContainer InfoEncodeSrv {
            get { return _infoEncodeContainer ?? (_infoEncodeContainer = GetService<IInfoEncodeContainer>()); }
        }

        #endregion


        #region IGetSalesReturnListService 接口成员
        /// <summary>
        /// 获取销退单
        /// </summary>
        /// <param name="programJobNo">作业编号  6.销退单</param>
        /// <param name="scanType">扫描类型1.箱条码 2.单据条码 3.料号</param>
        /// <param name="status">执行动作  A.新增  S.过帐</param>
        /// <param name="docNo">单据编号</param>
        /// <param name="id">ID</param>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        public DependencyObjectCollection GetSalesReturn(string programJobNo, string scanType, string status, string[] docNo, string id, string siteNo) {
            #region 参数检查
            if (Maths.IsEmpty(status)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "status"));//‘入参【status】未传值’
            }
            if (Maths.IsEmpty(docNo)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "docNo"));//‘入参【docNo】未传值’
            }
            if (Maths.IsEmpty(siteNo)) {
                throw new BusinessRuleException(InfoEncodeSrv.GetMessage("A111201", "site_no"));//‘入参【site_no】未传值’
            }
            #endregion

            QueryNode queryNode = GetSalesReturnQueryNode(programJobNo, scanType, status, docNo, id, siteNo);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }
        #endregion

        #region 业务员方法

        private QueryNode GetSalesReturnQueryNode(string programJobNo, string scanType, string status, string[] docNos, string id, string siteNo) {
            QueryConditionGroup conditionGroup = OOQL.CreateProperty("SALES_RETURN.CATEGORY") == OOQL.CreateConstants("26") &
                                                 OOQL.CreateProperty("SALES_RETURN.ApproveStatus") == OOQL.CreateConstants("Y") &
                                                 OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo);
            if (scanType == "1") {
                //箱条码
                conditionGroup &= (OOQL.CreateProperty("SALES_RETURN_D.SALES_RETURN_D_ID") == OOQL.CreateConstants(id));
            } else if (scanType == "2") {
                conditionGroup &= (OOQL.CreateProperty("SALES_RETURN.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNos)));
            }

            string docType = programJobNo + status;

            QueryNode queryNode =
                          OOQL.Select(true,
                                      OOQL.CreateConstants("99", "enterprise_no"),
                                      OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                                      OOQL.CreateConstants(programJobNo, "source_operation"),
                                      OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_CODE", "main_organization"),
                                      OOQL.CreateProperty("SALES_RETURN.DOC_NO", "source_no"),
                                      OOQL.CreateConstants(docType, "doc_type"),
                                      OOQL.CreateProperty("SALES_RETURN.DOC_DATE", "create_date"),
                                      OOQL.CreateProperty("SALES_RETURN_D.SequenceNumber", "seq"),
                                      Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Decimal, "doc_line_seq"),
                                      Formulas.Cast(OOQL.CreateConstants(0), GeneralDBType.Decimal, "doc_batch_seq"),
                                      OOQL.CreateConstants(string.Empty, "object_no"),
                                      OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                      Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty), "item_feature_no"),
                                      Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_feature_name"),
                                      Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(string.Empty), "warehouse_no"),
                                      Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty), "storage_spaces_no"),
                                      Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(string.Empty), "lot_no"),
                                      OOQL.CreateProperty("SALES_RETURN_D.BUSINESS_QTY", "doc_qty"),
                                      OOQL.CreateProperty("SALES_RETURN_D.RECEIPTED_BUSINESS_QTY", "in_out_qty"),
                                      Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "unit_no"),
                                      OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                      OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                      OOQL.CreateArithmetic(Formulas.IsNull(OOQL.CreateProperty("ITEM_SALES.GENERAL_DEL_OVERRUN_RATE"), OOQL.CreateConstants(0m))
                                        , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"),//20170302 modi by shenbao for P001-170302002 误差率统一乘100
                                      Formulas.IsNull(Formulas.Case(null, OOQL.CreateConstants("1"), new CaseItem[]{
                                                                    new CaseItem(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL")==OOQL.CreateConstants("N")
                                                                    ,OOQL.CreateConstants("2"))
                                                                  }), OOQL.CreateConstants(string.Empty), "lot_control_type"),  //批号管控方式
                                      Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator", new object[]{ OOQL.CreateProperty("SALES_RETURN_D.ITEM_ID")
                                                  , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                                  , OOQL.CreateProperty("SALES_RETURN_D.BUSINESS_UNIT_ID")
                                                  , OOQL.CreateConstants(1)}),  //单位转换率分母
                                      Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular", new object[]{ OOQL.CreateProperty("SALES_RETURN_D.ITEM_ID")
                                                  , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                                                  , OOQL.CreateProperty("SALES_RETURN_D.BUSINESS_UNIT_ID")
                                                  , OOQL.CreateConstants(0)}),  //单位转换率分
                                      Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(string.Empty), "inventory_unit"), //库存单位
                                      OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),//20170424 add by wangyq for P001-170420001
                                      OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type"),//20170424 add by wangyq for P001-170420001
                // modi by 08628 for P001-171023001 b
                                         OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_CODE", "main_warehouse_no"),
                                         OOQL.CreateProperty("MAIN_BIN.BIN_CODE", "main_storage_no"),
                                         OOQL.CreateConstants(string.Empty, "first_in_first_out_control")
                // modi by 08628 for P001-171023001 e
                                      )
                               .From("SALES_RETURN", "SALES_RETURN")
                               .InnerJoin("SALES_RETURN.SALES_RETURN_D", "SALES_RETURN_D")
                               .On(OOQL.CreateProperty("SALES_RETURN_D.SALES_RETURN_ID") == OOQL.CreateProperty("SALES_RETURN.SALES_RETURN_ID"))
                               .InnerJoin("SALES_CENTER", "SALES_CENTER")
                               .On(OOQL.CreateProperty("SALES_CENTER.SALES_CENTER_ID") == OOQL.CreateProperty("SALES_RETURN.Owner_Org.ROid"))
                               .InnerJoin("PLANT", "PLANT")
                               .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("SALES_RETURN.PLANT_ID"))
                               .InnerJoin("ITEM", "ITEM")
                               .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("SALES_RETURN_D.ITEM_ID"))
                               .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                               .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("SALES_RETURN_D.ITEM_FEATURE_ID"))
                               .LeftJoin("WAREHOUSE", "WAREHOUSE")
                               .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("SALES_RETURN_D.WAREHOUSE_ID"))
                               .LeftJoin("WAREHOUSE.BIN", "BIN")
                               .On(OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("SALES_RETURN_D.BIN_ID"))
                               .LeftJoin("ITEM_LOT", "ITEM_LOT")
                               .On(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("SALES_RETURN_D.ITEM_LOT_ID"))
                               .LeftJoin("UNIT", "UNIT")
                               .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("SALES_RETURN_D.BUSINESS_UNIT_ID"))
                               .LeftJoin("ITEM_PLANT")
                               .On(OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("SALES_RETURN_D.ITEM_ID") &
                                   OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("SALES_RETURN.PLANT_ID"))
                               .LeftJoin("ITEM_SALES", "ITEM_SALES")
                               .On(OOQL.CreateProperty("ITEM_SALES.ITEM_ID") == OOQL.CreateProperty("SALES_RETURN_D.ITEM_ID") &
                                   OOQL.CreateProperty("ITEM_SALES.Owner_Org.ROid") == OOQL.CreateProperty("SALES_RETURN.Owner_Org.ROid"))
                               .LeftJoin("UNIT", "STOCK_UNIT")
                               .On(OOQL.CreateProperty("STOCK_UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"))
                // modi by 08628 for P001-171023001 b
                           .LeftJoin("WAREHOUSE", "MAIN_WAREHOUSE")
                           .On(OOQL.CreateProperty("MAIN_WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"))
                           .LeftJoin("WAREHOUSE.BIN", "MAIN_BIN")
                           .On(OOQL.CreateProperty("MAIN_BIN.WAREHOUSE_ID") == OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID")
                           & OOQL.CreateProperty("MAIN_BIN.MAIN") == OOQL.CreateConstants(1))
                // modi by 08628 for P001-171023001 e
                               .Where(conditionGroup);
            return queryNode;
        }

        #endregion
    }
}