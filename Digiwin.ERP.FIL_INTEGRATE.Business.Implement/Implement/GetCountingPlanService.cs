//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>shenbao</author>
//<createDate>2017/02/03 09:19:37</createDate>
//<IssueNo>P001-170124001</IssueNo>
//<description>获取盘点计划服务</description>
//20170209 modi by wangyq for B001-170206023 
//20170302 modi by shenbao for B001-170221021 盘点计划为空时，获取所有的品号条码
//20170302 modi by shenbao for P001-170302002 增加条码数量
//20170405 modi by wangrm SD口述需求 下载全部条码

using System.Collections;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement
{
    [ServiceClass(typeof (IGetCountingPlanService))]
    [SingleGetCreator]
    public sealed class GetCountingPlanService : ServiceComponent, IGetCountingPlanService
    {
        /// <summary>
        /// 获取盘点计划
        /// </summary>
        /// <param name="counting_type">盘点类型</param>
        /// <param name="warehouse_no">仓库</param>
        /// <param name="counting_no">盘点计划编号</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="barcode_no">条码编号</param>
        /// <returns></returns>
        public Hashtable GetCountingPlan(string counting_type, string warehouse_no, string counting_no, string site_no,
            string barcode_no)
        {
            #region 参数检查

            //20170209 mark by wangyq for B001-170206023  ============begin============
            //if (Maths.IsEmpty(warehouse_no)) {
            //    var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
            //    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "warehouse_no"));//‘入参【warehouse_no】未传值’
            //}
            //20170209 mark by wangyq for B001-170206023  ============end============

            if (Maths.IsEmpty(counting_no))
            {
                var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "counting_no"));
                    //‘入参【counting_no】未传值’
            }

            #endregion

            Hashtable hashTable = new Hashtable();
            DependencyObjectCollection source = QuerySource(counting_type, counting_no, site_no);
                //20170209 modi by wangyq for B001-170206023 去除仓库warehouse_no,
            hashTable.Add("source_doc_detail", source);
            //20170405 mark by wangrm SD口述需求 下载全部条码====start=====
            //DependencyObjectCollection barcode = QueryBarCode(counting_type, counting_no, site_no);//20170209 modi by wangyq for B001-170206023 去除仓库warehouse_no,
            ////20170302 add by shenbao for B001-170221021 ===begin===
            //if (barcode.Count == 0)
            //    barcode = QueryBCRecord(site_no);
            //20170302 add by shenbao for B001-170221021 ===end===
            //20170405 mark by wangrm SD口述需求 下载全部条码====end=====
            DependencyObjectCollection barcode = QueryBCRecord(site_no, barcode_no);
                //20170405 add by wangrm SD口述需求 下载全部条码
            hashTable.Add("barcode_detail", barcode);

            return hashTable;
        }

        public DependencyObjectCollection QuerySource(string counting_type, string counting_no, string site_no)
        {
//20170209 modi by wangyq for B001-170206023 去除仓库 string warehouse_no,
            QueryNode node = OOQL.Select(OOQL.CreateConstants("99", "enterprise_no") //企业编号
                , OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no") //营运据点
                , OOQL.CreateProperty("PLANT.PLANT_ID", "plant_id") //工厂
                , OOQL.CreateProperty("COUNTING_PLAN.DOC_NO", "counting_no") //盘点计划编号
                , OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no") //料件编号
                ,
                OOQL.CreateArithmetic(OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION")
                    , ArithmeticOperators.Plus, "item_name_spec") //品名
                ,
                Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(string.Empty), "item_feature_no") //产品特征码
                ,
                Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(string.Empty),
                    "warehouse_no") //库位
                ,
                Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty),
                    "storage_spaces_no") //储位
                ,
                Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(string.Empty), "lot_no")
                //批号
                , OOQL.CreateProperty("COUNTING_PLAN.BO_ID.RTK", "transaction_type") //交易对象类型
                , Formulas.Case(OOQL.CreateProperty("COUNTING_PLAN.BO_ID.RTK"), OOQL.CreateConstants(""), new CaseItem[]
                {
                    new CaseItem(OOQL.CreateConstants("CUSTOMER"), OOQL.CreateProperty("CUSTOMER.CUSTOMER_CODE")),
                    new CaseItem(OOQL.CreateConstants("SUPPLIER"), OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE")),
                    new CaseItem(OOQL.CreateConstants("EMPLOYEE"), OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE")),
                    new CaseItem(OOQL.CreateConstants("OTHER_BO"), OOQL.CreateProperty("OTHER_BO.OTHER_BO_CODE")),
                }, "transaction_no") //交易对象编号
                , OOQL.CreateProperty("COUNTING_PLAN.BOOK_QTY", "inventory_qty") //库存数量
                )
                .From("COUNTING_PLAN", "COUNTING_PLAN")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("COUNTING_PLAN.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("COUNTING_PLAN.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("COUNTING_PLAN.ITEM_FEATURE_ID") ==
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("COUNTING_PLAN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("COUNTING_PLAN.BIN_ID") == OOQL.CreateProperty("BIN.BIN_ID"))
                .LeftJoin("ITEM_LOT")
                .On(OOQL.CreateProperty("COUNTING_PLAN.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                .LeftJoin("CUSTOMER")
                .On(OOQL.CreateProperty("COUNTING_PLAN.BO_ID.ROid") == OOQL.CreateProperty("CUSTOMER.CUSTOMER_ID"))
                .LeftJoin("SUPPLIER")
                .On(OOQL.CreateProperty("COUNTING_PLAN.BO_ID.ROid") == OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID"))
                .LeftJoin("EMPLOYEE")
                .On(OOQL.CreateProperty("COUNTING_PLAN.BO_ID.ROid") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"))
                .LeftJoin("OTHER_BO")
                .On(OOQL.CreateProperty("COUNTING_PLAN.BO_ID.ROid") == OOQL.CreateProperty("OTHER_BO.OTHER_BO_ID"))
                .Where(OOQL.AuthFilter("COUNTING_PLAN", "COUNTING_PLAN")
                       & (OOQL.CreateProperty("COUNTING_PLAN.DOC_NO") == OOQL.CreateConstants(counting_no)
                          &
                          (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no) |
                           OOQL.CreateConstants(site_no) == OOQL.CreateConstants(string.Empty))
                           //& OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateConstants(warehouse_no)//20170209 mark by wangyq for B001-170206023 新需求
                          & OOQL.CreateProperty("COUNTING_PLAN.ApproveStatus") == OOQL.CreateConstants("N")));

            return this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        public DependencyObjectCollection QueryBarCode(string counting_type, string counting_no, string site_no,
            string barcode_no)
        {
            //20170209 modi by wangyq for B001-170206023 去除仓库 string warehouse_no,
            object propertyID = QueryItemLot();
            object qtPropertyID = QueryItemQty(); //20170302 add by shenbao for P001-170302002

            QueryNode node = OOQL.Select(true, OOQL.CreateConstants("99", "enterprise_no") //企业编号
                , OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no") //营运据点
                , OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no") //条码编号
                , OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no") //料件编号
                ,
                OOQL.CreateArithmetic(OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION")
                    , ArithmeticOperators.Plus, "item_name_spec") //品名
                ,
                Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(string.Empty), "item_feature_no") //产品特征码
                , OOQL.CreateConstants(string.Empty, "warehouse_no") //库位
                , OOQL.CreateConstants(string.Empty, "storage_spaces_no") //储位
                ,
                Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"), OOQL.CreateConstants(string.Empty),
                    "lot_no") //批号
                //, OOQL.CreateConstants(0m, "inventory_qty")  //数量  //20170302 mark by shenbao for P001-170302002
                ,
                Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D_QTY.BC_PROPERTY_VALUE"), OOQL.CreateConstants(0),
                    "barcode_qty") //20170302 ADD by shenbao for P001-170302002
                )
                .From("COUNTING_PLAN", "COUNTING_PLAN")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("COUNTING_PLAN.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("COUNTING_PLAN.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("COUNTING_PLAN.ITEM_FEATURE_ID") ==
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("COUNTING_PLAN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                .InnerJoin("BC_RECORD")
                .On(OOQL.CreateProperty("COUNTING_PLAN.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")
                    &
                    OOQL.CreateProperty("COUNTING_PLAN.ITEM_FEATURE_ID") ==
                    OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID"))
                .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                //20170209 modi by wangyq for B001-170206023 old:InnerJoin
                .On(OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID")
                    & OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_ID") == OOQL.CreateConstants(propertyID))
                //20170302 add by shenbao for P001-170302002 ===begin===
                .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D_QTY")
                .On(OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD_D_QTY.BC_RECORD_ID")
                    & OOQL.CreateProperty("BC_RECORD_D_QTY.BC_PROPERTY_ID") == OOQL.CreateConstants(qtPropertyID))
                //20170302 add by shenbao for P001-1703020002 ===end===
                .Where(OOQL.AuthFilter("COUNTING_PLAN", "COUNTING_PLAN")
                       & (OOQL.CreateProperty("COUNTING_PLAN.DOC_NO") == OOQL.CreateConstants(counting_no)
                          &
                          (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no) |
                           OOQL.CreateConstants(site_no) == OOQL.CreateConstants(string.Empty))
                // 20171023 add by 08628 for P001-171023001 ↓
                           & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcode_no)
                           | OOQL.CreateConstants(barcode_no) == OOQL.CreateConstants(string.Empty))
                // 20171023 add by 08628 for P001-171023001 ↑
                           //& OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateConstants(warehouse_no)//20170209 mark by wangyq for B001-170206023 去除仓库
                          & OOQL.CreateProperty("COUNTING_PLAN.ApproveStatus") == OOQL.CreateConstants("N")));

            return this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        //20170302 add by shenbao for B001-170221021
        private DependencyObjectCollection QueryBCRecord(string siteNo, string barcode_no)
        {
            object propertyID = QueryItemLot();
            object qtPropertyID = QueryItemQty(); //20170302 add by shenbao for P001-170302002
            QueryNode node = OOQL.Select(true, OOQL.CreateConstants("99", "enterprise_no") //企业编号
                , OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no") //营运据点
                , OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no") //条码编号
                , OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no") //料件编号
                ,
                OOQL.CreateArithmetic(OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION")
                    , ArithmeticOperators.Plus, "item_name_spec") //品名
                ,
                Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(string.Empty), "item_feature_no") //产品特征码
                , OOQL.CreateConstants(string.Empty, "warehouse_no") //库位
                , OOQL.CreateConstants(string.Empty, "storage_spaces_no") //储位
                ,
                Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"), OOQL.CreateConstants(string.Empty),
                    "lot_no") //批号
                //, OOQL.CreateConstants(0m, "inventory_qty")  //数量  //20170302 mark by shenbao for P001-170302002
                ,
                Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D_QTY.BC_PROPERTY_VALUE"), OOQL.CreateConstants(0),
                    "barcode_qty") //20170302 ADD by shenbao for P001-170302002
                )
                .From("BC_RECORD", "BC_RECORD")
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("BC_RECORD.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID") ==
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                .On(OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID")
                    & OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_ID") == OOQL.CreateConstants(propertyID))
                //20170302 add by shenbao for P001-170302002 ===begin===
                .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D_QTY")
                .On(OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD_D_QTY.BC_RECORD_ID")
                    & OOQL.CreateProperty("BC_RECORD_D_QTY.BC_PROPERTY_ID") == OOQL.CreateConstants(qtPropertyID))
                //20170302 add by shenbao for P001-1703020002 ===end===
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID"))
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD")
                        & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                           | OOQL.CreateConstants(siteNo) == OOQL.CreateConstants(string.Empty))
                // 20171023 add by 08628 for P001-171023001 ↓
                           & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcode_no)
                           | OOQL.CreateConstants(barcode_no) == OOQL.CreateConstants(string.Empty))
                // 20171023 add by 08628 for P001-171023001 ↑
                           ));

            return this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        private object QueryItemLot()
        {
            QueryNode node = OOQL.Select("BC_PROPERTY.BC_PROPERTY_ID")
                .From("BC_PROPERTY")
                .Where(OOQL.CreateProperty("BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot"));

            object result = this.GetService<IQueryService>().ExecuteScalar(node);
            result = result == null ? Maths.GuidDefaultValue() : result;

            return result;
        }

        //20170302 add by shenbao for P001-170302002
        private object QueryItemQty()
        {
            QueryNode node = OOQL.Select("BC_PROPERTY.BC_PROPERTY_ID")
                .From("BC_PROPERTY")
                .Where(OOQL.CreateProperty("BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty"));

            object result = this.GetService<IQueryService>().ExecuteScalar(node);
            result = result == null ? Maths.GuidDefaultValue() : result;

            return result;
        }
    }
}