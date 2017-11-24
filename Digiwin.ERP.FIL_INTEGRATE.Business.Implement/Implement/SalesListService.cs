//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-12</createDate>
//<description>销货清单服务</description>
//---------------------------------------------------------------- 
//20171010 modi by zhangcn for B001-171010004	回传增加特征码规格

using System;
using System.Collections;
using System.ComponentModel;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    [ServiceClass(typeof(ISalesListService))]
    [Description("销货清单服务")]
    public class SalesListService : ServiceComponent, ISalesListService {

        #region ISalesListService 成员
        /// <summary>
        /// 根据传入的供应商查询出所有的销货清单
        /// </summary>
        /// <param name="supplier_no">供应商编号(必填)</param>
        /// <param name="date_s">对账开始日期</param>
        /// <param name="date_e">对账截止日期</param>
        /// <param name="enterprise_no">企业别</param>
        /// <param name="site_no">营运中心</param>
        /// <returns></returns>
        public Hashtable SalesList(string supplier_no, string date_s, string date_e, string enterprise_no, string site_no) {
            try {
                if (Maths.IsEmpty(supplier_no)) {
                    IInfoEncodeContainer infoEnCode = GetService<IInfoEncodeContainer>();
                    throw new BusinessRuleException(infoEnCode.GetMessage("A111201", "supplier_no"));//‘入参【supplier_no】未传值’( A111201)
                }

                //获取销货清单
                QueryNode queryNode = GetSalesListDetail(supplier_no, date_s, date_e, site_no);
                DependencyObjectCollection entity = GetService<IQueryService>().ExecuteDependencyObject(queryNode);
                
                //返回结果
                Hashtable result = new Hashtable();
                if (entity.Count>0) {
                    result.Add("supplier_no", entity[0]["supplier_no"]);//销货方
                    result.Add("supplier_name", entity[0]["supplier_name"]);//销货方名称
                    result.Add("employee_no", entity[0]["employee_no"]);//联系人
                    result.Add("supplier_telephone_number", entity[0]["supplier_telephone_number"]);//联系电话
                    result.Add("site_no", entity[0]["site_no"]);//进货方
                    result.Add("site_name", entity[0]["site_name"]);//进货名称
                    result.Add("telephone_number", entity[0]["telephone_number"]);//联系电话
                } else {
                    result.Add("supplier_no", string.Empty);//销货方
                    result.Add("supplier_name", string.Empty);//销货方名称
                    result.Add("employee_no", string.Empty);//联系人
                    result.Add("supplier_telephone_number", string.Empty);//联系电话
                    result.Add("site_no", string.Empty);//进货方
                    result.Add("site_name", string.Empty);//进货名称
                    result.Add("telephone_number", string.Empty);//联系电话
                }
                //需要移除的列
                string[] excludeColumns ={
                                        "supplier_no",
                                        "supplier_name",
                                        "employee_no",
                                        "supplier_telephone_number",
                                        "site_no", 
                                        "site_name",
                                        "telephone_number"
                };
                //重新组合返回结果
                DependencyObjectCollection newEntity = RemoveObjectType(entity, excludeColumns);

                result.Add("sales_list_detail", newEntity);//销货清单
                return result;
            } catch (Exception) {
                throw;
            }
        }

        #endregion

        /// <summary>
        /// 移除不需要的列
        /// </summary>
        /// <param name="entity">实体集合</param>
        /// <param name="excludeColumns">需要移除的列</param>
        /// <returns></returns>
        private DependencyObjectCollection RemoveObjectType(DependencyObjectCollection entity, string[] excludeColumns) {
            DependencyObjectType cloneObjectType = new DependencyObjectType("collection1_clone");
            IDataEntityType dataEntityType = entity.ItemDependencyObjectType;
            foreach (ISimpleProperty property in dataEntityType.SimpleProperties) {
                if (!excludeColumns.Contains(property.Name)) {
                    cloneObjectType.RegisterSimpleProperty(property.Name, property.PropertyType);
                }
            }
            DependencyObjectCollection cloneObject = new DependencyObjectCollection(cloneObjectType);//克隆对象
            foreach (DependencyObject item in entity) {
                var newItem = cloneObject.AddNew();
                foreach (ISimpleProperty property in ((IDataEntityType)item.DependencyObjectType).SimpleProperties) {
                    if (newItem.DependencyObjectType.Properties.Contains(property.Name)) {
                        newItem[property.Name] = item[property.Name];
                    }
                }
            }
            return cloneObject;
        }

        private QueryNode GetSalesListDetail(string supplierNo, string dateS, string dateE, string siteNo) {
            JoinOnNode joinOnNode=
                OOQL.Select(
                            OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE", "supplier_no"),
                            OOQL.CreateProperty("SUPPLIER.SUPPLIER_NAME", "supplier_name"),
                            OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_CONTACT_NAME", "employee_no"),
                            Formulas.IsNull(
                                    OOQL.CreateProperty("SUPPLIER_CONTACT.TELEPHONE"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "supplier_telephone_number"),
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                            OOQL.CreateProperty("PLANT.PLANT_NAME", "site_name"),
                            OOQL.CreateProperty("PLANT.TELEPHONE", "telephone_number"),
                            OOQL.CreateProperty("PURCHASE_RECEIPT.TRANSACTION_DATE", "create_date"),
                            OOQL.CreateProperty("PURCHASE_RECEIPT.DOC_NO", "stock_in_no"),
                            OOQL.CreateProperty("PURCHASE_RECEIPT_D.SequenceNumber", "seq"),
                            OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                            OOQL.CreateProperty("PURCHASE_RECEIPT_D.ITEM_DESCRIPTION", "item_name"),
                            OOQL.CreateProperty("PURCHASE_RECEIPT_D.ITEM_SPECIFICATION", "item_spec"),
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_no"),   //20171010 add by zhangcn for B001-171010004
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(string.Empty, GeneralDBType.String), "item_feature_name"),//20171010 add by zhangcn for B001-171010004
                            Formulas.IsNull(
                                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String), "unit_no"),
                            OOQL.CreateProperty("PURCHASE_RECEIPT_D.BUSINESS_QTY", "qty"),
                            OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PRICE", "price"),
                            OOQL.CreateArithmetic(
                                    OOQL.CreateProperty("PURCHASE_RECEIPT_D.AMOUNT_UNINCLUDE_TAX_OC"),
                                    OOQL.CreateProperty("PURCHASE_RECEIPT_D.TAX_OC"), ArithmeticOperators.Plus, "tax_amount"),
                            OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "purchase_no"),
                            OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber", "purchase_seq"))
                        .From("PURCHASE_RECEIPT", "PURCHASE_RECEIPT")
                        .InnerJoin("PURCHASE_RECEIPT.PURCHASE_RECEIPT_D", "PURCHASE_RECEIPT_D")
                        .On((OOQL.CreateProperty("PURCHASE_RECEIPT_D.PURCHASE_RECEIPT_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT.PURCHASE_RECEIPT_ID")))
                        .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                        .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.ORDER_SOURCE_ID.ROid")))
                        .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                        .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID")))
                        .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                        .On((OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID")))
                        .InnerJoin("SUPPLIER_PURCHASE", "SUPPLIER_PURCHASE")
                        .On((OOQL.CreateProperty("SUPPLIER_PURCHASE.Owner_Org.ROid") == OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid"))
                                     & (OOQL.CreateProperty("SUPPLIER_PURCHASE.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.SUPPLIER_ID")))
                        .LeftJoin("SUPPLIER_PURCHASE.SUPPLIER_CONTACT", "SUPPLIER_CONTACT")
                        .On((OOQL.CreateProperty("SUPPLIER_CONTACT.SUPPLIER_BUSINESS_ID") == OOQL.CreateProperty("SUPPLIER_PURCHASE.SUPPLIER_PURCHASE_ID"))
                                     & (OOQL.CreateProperty("SUPPLIER_CONTACT.MIAN_ORDER_CONTACT") == OOQL.CreateConstants(1, GeneralDBType.Int32)))
                        .InnerJoin("PLANT", "PLANT")
                        .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT.Owner_Org.ROid")))
                        .InnerJoin("ITEM", "ITEM")
                        .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.ITEM_ID")))
                        .LeftJoin("UNIT", "UNIT")
                        .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.PRICE_UNIT_ID")))
                        .InnerJoin("SUPPLIER", "SUPPLIER")
                        .On((OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT.SUPPLIER_ID")))
                        .InnerJoin("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                        .On((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID") == OOQL.CreateProperty("PURCHASE_RECEIPT_D.SOURCE_ID")))
                //20171010 add by zhangcn for B001-171010004  ========================begin=========================
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("PURCHASE_RECEIPT_D.ITEM_FEATURE_ID") ==
                        OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"));
                //20171010 add by zhangcn for B001-171010004  ========================end=========================
            
            //如果结束日期为空格，空值，null时默认最大日期
            if (Maths.IsEmpty(dateE.ToDate())){
                dateE = OrmDataOption.EmptyDateTime1.ToStringExtension();
            }
            //如果开始日期为空格，空值，null时默认最小日期
            if (Maths.IsEmpty(dateS.ToDate())){
                dateS = OrmDataOption.EmptyDateTime.ToStringExtension();
            }
            //初始Where条件
            QueryConditionGroup conditionGroup = (OOQL.AuthFilter("PURCHASE_RECEIPT", "PURCHASE_RECEIPT"))
                                                & (OOQL.CreateProperty("PURCHASE_RECEIPT.ApproveStatus") == OOQL.CreateConstants("Y"))
                                                & (OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateConstants(supplierNo))
                                                & (OOQL.CreateProperty("PURCHASE_RECEIPT.TRANSACTION_DATE") >= OOQL.CreateConstants(dateS.ToDate()))
                                                & (OOQL.CreateProperty("PURCHASE_RECEIPT.TRANSACTION_DATE") <= OOQL.CreateConstants(dateE.ToDate()));

            //如果营运中心不为空增加条件
            if (!Maths.IsEmpty(siteNo)) {
                conditionGroup &= (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo));
            }

            return joinOnNode.Where(conditionGroup);
        }
    }
}
