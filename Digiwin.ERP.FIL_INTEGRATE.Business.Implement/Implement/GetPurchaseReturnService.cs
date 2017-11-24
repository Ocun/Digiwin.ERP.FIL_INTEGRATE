//---------------------------------------------------------------- 
//Copyright (C) 2016-2017 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>wangrm</author>
//<createDate>2017-2-10</createDate>
//<IssueNo>P001-170207001</IssueNo>
//<description>获取采购退货单通知服务实现</description>
//----------------------------------------------------------------
//20170424 modi by wangyq for P001-170420001

using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetPurchaseReturnService))]
    [Description("获取销货单通知服务")]
    public class GetPurchaseReturnService : ServiceComponent, IGetPurchaseReturnService {
        #region IGetPurchaseReturnService 成员

        public DependencyObjectCollection GetPurchaseReturn(string programJobNo, string scanType, string status, string[] docNo, string ID, string siteNo) {
            #region 参数检查
            if (Maths.IsEmpty(status)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "status"));//‘入参【status】未传值’//message A111201
            }
            if (Maths.IsEmpty(docNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "doc_no"));//‘入参【doc_no】未传值’
            }
            if (Maths.IsEmpty(siteNo)) {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "site_no"));//‘‘入参【site_no】未传值’//message A111201
            }
            #endregion

            //查询销货单
            QueryNode queryNode = GetPurchaseReturnNode(programJobNo, scanType, ID, siteNo, status, docNo);
            return GetService<IQueryService>().ExecuteDependencyObject(queryNode);
        }

        #endregion

        #region 业务方法
        private QueryNode GetPurchaseReturnNode(string programJobNo, string scanType, string ID, string siteNo, string status, string[] docNo) {
            string docType = programJobNo + status;
            QueryConditionGroup whereNode = (OOQL.AuthFilter("PURCHASE_RETURN", "PURCHASE_RETURN")) &
                 (OOQL.CreateProperty("PURCHASE_RETURN.CATEGORY") == OOQL.CreateConstants("39")
                 & OOQL.CreateProperty("PURCHASE_RETURN.ApproveStatus") == OOQL.CreateConstants("Y")
                 & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                 & OOQL.CreateProperty("PURCHASE_RETURN_D.ISSUE_CLOSE") == OOQL.CreateConstants("0"));
            switch (scanType) {
                case "1":
                    whereNode &= OOQL.CreateProperty("PURCHASE_RETURN_D.PURCHASE_RETURN_D_ID") == OOQL.CreateConstants(ID);
                    break;
                case "2":
                    whereNode &= OOQL.CreateProperty("PURCHASE_RETURN.DOC_NO").In(OOQL.CreateDyncParameter("DOC_NO", docNo));
                    break;
            }
            QueryNode node = OOQL.Select(OOQL.CreateConstants("99", "enterprise_no"),
                                         OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                                         OOQL.CreateConstants(programJobNo, "source_operation"),
                                         OOQL.CreateProperty("PURCHASE_RETURN.DOC_NO", "source_no"),
                                         OOQL.CreateConstants(docType, "doc_type"),
                                         OOQL.CreateProperty("PURCHASE_RETURN.DOC_DATE", "create_date"),
                                         OOQL.CreateProperty("PURCHASE_RETURN_D.SequenceNumber", "seq"),
                                         OOQL.CreateConstants(0, "doc_line_seq"),
                                         OOQL.CreateConstants(0, "doc_batch_seq"),
                                         OOQL.CreateConstants(string.Empty, "object_no"),
                                         OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                         Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty), "item_feature_no"),
                                         Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty), "item_feature_name"),
                                         Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(string.Empty), "warehouse_no"),
                                         Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty), "storage_spaces_no"),
                                         Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(string.Empty), "lot_no"),
                                         OOQL.CreateProperty("PURCHASE_RETURN_D.BUSINESS_QTY", "doc_qty"),
                                         OOQL.CreateProperty("PURCHASE_RETURN_D.ISSUED_BUSINESS_QTY", "in_out_qty"),
                                         OOQL.CreateProperty("UNIT.UNIT_CODE", "unit_no"),
                                         OOQL.CreateArithmetic(Formulas.IsNull(OOQL.CreateProperty("ITEM_PURCHASE.RECEIPT_OVER_RATE"), OOQL.CreateConstants(0))
                                            , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"),//20170302 modi by shenbao for P001-170302002 误差率统一乘100
                                         OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                         OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                         Formulas.Case(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL"), OOQL.CreateConstants("1"), new CaseItem[] { new CaseItem(OOQL.CreateConstants("N"), OOQL.CreateConstants("2")) }, "lot_control_type"),
                                         Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator",
                                new object[]{
                                    OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID"),
                                    OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                    OOQL.CreateProperty("PURCHASE_RETURN_D.BUSINESS_UNIT_ID"),
                                    OOQL.CreateConstants(1)
                                }), //单位转换率分母
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular",
                                new object[]{
                                    OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID"),
                                    OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                    OOQL.CreateProperty("PURCHASE_RETURN_D.BUSINESS_UNIT_ID"),
                                    OOQL.CreateConstants(0)
                                }), //单位转换率分子
                                         OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE", "inventory_unit"),
                                         OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE", "main_organization"),
                                         OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),//20170424 add by wangyq for P001-170420001
                                         OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type")//20170424 add by wangyq for P001-170420001
                            )
                .From("PURCHASE_RETURN", "PURCHASE_RETURN")
                .InnerJoin("PURCHASE_RETURN.PURCHASE_RETURN_D", "PURCHASE_RETURN_D")
                .On(OOQL.CreateProperty("PURCHASE_RETURN.PURCHASE_RETURN_ID") == OOQL.CreateProperty("PURCHASE_RETURN_D.PURCHASE_RETURN_ID"))
                .InnerJoin("SUPPLY_CENTER", "SUPPLY_CENTER")
                .On(OOQL.CreateProperty("PURCHASE_RETURN.Owner_Org.ROid") == OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID"))
                .InnerJoin("PLANT", "PLANT")
                .On(OOQL.CreateProperty("PURCHASE_RETURN.PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM", "ITEM")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("WAREHOUSE", "WAREHOUSE")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.BIN_ID") == OOQL.CreateProperty("BIN.BIN_ID"))
                .LeftJoin("ITEM_LOT", "ITEM_LOT")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                .LeftJoin("UNIT", "UNIT")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                 & OOQL.CreateProperty("PURCHASE_RETURN.PLANT_ID") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                 .LeftJoin("ITEM_PURCHASE", "ITEM_PURCHASE")
                 .On(OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID")
                 & OOQL.CreateProperty("PURCHASE_RETURN.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid"))
                 .LeftJoin("UNIT", "STOCK_UNIT")
                 .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                 .Where(whereNode);
            return node;
        }
        #endregion
    }
}
