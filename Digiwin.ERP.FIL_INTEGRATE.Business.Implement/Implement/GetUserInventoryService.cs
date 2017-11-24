//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-17</createDate>
//<description>获取使用者库存信息实现</description>
//----------------------------------------------------------------
//20161215 modi by liwei1 for B001-161215015
//20161216 modi by shenbao for P001-161215001
//20161215 modi by liwei1 for P001-161215001
//20170303 modi by liwei1 for B001-170303008 增加回传参数：是否做储位管理
//20160309 modi by shenbao for B001-170309014
//20170331 modi by wangyq for P001-170327001

using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.Mars.Integration;
using Digiwin.Common.Services;
using Digiwin.ERP.Common.Utils;


namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetUserInventoryService))]
    [Description("获取使用者库存信息实现")]
    public class GetUserInventoryService : ServiceComponent, IGetUserInventoryService {

        #region IGetUserInventoryService 成员
        /// <summary>
        /// 获取相应的用户信息
        /// </summary>
        /// <param name="hashkey">使用者账号、密码</param>
        /// <param name="report_datetime">上传时间：暂时没有启用这个参数</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        public Hashtable GetUserInventory(string hashkey, string report_datetime, string site_no) {
            try {
                if (Maths.IsEmpty(hashkey)) {
                    var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "hashkey"));//‘入参【hashkey】未传值’
                } else {
                    IFilIntegrationService filIntegrationSrv = this.GetService<IFilIntegrationService>("FIL_Integration");
                    //获取登陆用户名
                    string logonName = filIntegrationSrv.VerifyPwd(hashkey);

                    //获取用户信息
                    QueryNode queryNode = GetUserInfomation(logonName);
                    DependencyObjectCollection userInfomation = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                    //获取库存信息
                    queryNode = GetStockInfomation(logonName, report_datetime);
                    DependencyObjectCollection stockInfomation = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                    //组织返回结果
                    Hashtable result = new Hashtable();
                    result.Add("user_infomation", userInfomation);
                    result.Add("stock_infomation", stockInfomation);
                    return result;
                }
            } catch (Exception) {
                throw;
            }
        }



        #endregion
        /// <summary>
        /// 获取库存信息
        /// </summary>
        /// <param name="logonName"></param>
        /// <returns></returns>
        private QueryNode GetStockInfomation(string logonName, string reportTime) {
            //20160309 add by shenbao for B001-170309014 ===begin===
            QueryCondition condition = null;
            if (Maths.IsNotEmpty(reportTime))
                condition = OOQL.CreateProperty("WAREHOUSE.LastModifiedDate") > OOQL.CreateConstants(reportTime);
            else
                condition = OOQL.CreateConstants(1) == OOQL.CreateConstants(1);
            //20160309 add by shenbao for B001-170309014 ===end===
            return OOQL.Select(true,
                                    OOQL.CreateConstants("Y", GeneralDBType.String, "status"),
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String), "warehouse_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_NAME"),
                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String), "warehouse_name"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("WAREHOUSE.BIN_CODE"),
                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String), "storage_spaces_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("WAREHOUSE.BIN_NAME"),
                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String), "storage_spaces_name")
                //20170303 add by liwei1 for B001-170303008  ===begin===
                                    , Formulas.IsNull(
                                        OOQL.CreateProperty("WAREHOUSE.BIN_CONTROL"),
                                        OOQL.CreateConstants(string.Empty, GeneralDBType.String), "storage_spaces"))
                //20170303 add by liwei1 for B001-170303008  ===end===
                                .From("USER", "USER")
                                .InnerJoin("USER.USER_ORG", "USER_ORG")
                                .On((OOQL.CreateProperty("USER_ORG.USER_ID") == OOQL.CreateProperty("USER.USER_ID")))
                                .InnerJoin("PLANT", "PLANT")
                                .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("USER_ORG.ORG.ROid")))
                                .InnerJoin(  //20160309 MODI by shenbao for B001-170309014 改成innerjoin
                                    OOQL.Select(
                                            OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid", "Owner_Org_ROid"),
                                            OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),
                                            OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_NAME"),
                                            OOQL.CreateProperty("BIN.BIN_CODE"),
                                            OOQL.CreateProperty("BIN.BIN_NAME")
                //20170303 add by liwei1 for B001-170303008  ===begin===
                                            , Formulas.Case(null,
                                                    OOQL.CreateConstants("N", GeneralDBType.String),
                                                    OOQL.CreateCaseArray(
                                                            OOQL.CreateCaseItem((OOQL.CreateProperty("WAREHOUSE.BIN_CONTROL") == OOQL.CreateConstants("1")),
                                                                    OOQL.CreateConstants("Y", GeneralDBType.String))), "BIN_CONTROL")
                //20170303 add by liwei1 for B001-170303008  ===end===
                                            )
                                        .From("WAREHOUSE", "WAREHOUSE")
                                        .LeftJoin("WAREHOUSE.BIN", "BIN")
                                        .On((OOQL.CreateProperty("BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                                            & (OOQL.CreateProperty("WAREHOUSE.BIN_CONTROL") == OOQL.CreateConstants("1")))
                                        .Where(condition), "WAREHOUSE")  //20160309 add by shenbao for B001-170309014 添加条件condition
                                .On((OOQL.CreateProperty("WAREHOUSE.Owner_Org_ROid") == OOQL.CreateProperty("USER_ORG.ORG.ROid")))
                                .Where((OOQL.AuthFilter("USER", "USER"))
                                    & ((OOQL.CreateProperty("USER.LOGONNAME") == OOQL.CreateConstants(logonName))
                                    & (OOQL.CreateProperty("USER_ORG.ORG.RTK") == OOQL.CreateConstants("PLANT"))));
        }

        /// <summary>
        /// 获取用户信息
        /// </summary>
        /// <param name="logonName">用户名</param>
        /// <returns></returns>
        private QueryNode GetUserInfomation(string logonName) {
            //20170331 modi by wangyq for P001-170327001  ===============begin==============
            string[] paraList = GetParaFIL();
            ////20161216 add by shenbao for P001-161215001 ===begin===
            ////是否启用条码库存管理
            //bool isInvertory = UtilsClass.IsBCInventoryManagement(this.GetService<IQueryService>());
            //string manageBarCode = isInvertory ? "Y" : "N";
            ////20161216 add by shenbao for P001-161215001 ===end===
            //20170331 modi by wangyq for P001-170327001  ===============end==============
            return OOQL.Select(true,
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "code"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String, "sql_code"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String, "description"),
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                                    OOQL.CreateProperty("USER.LOGONNAME", "account"),
                                    OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE", "employee_no"),//20161215 add by liwei1 for P001-161215001
                //OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE", "employee_name"),//20161215 mark by liwei1 for P001-161215001
                                    OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_NAME", "employee_name"),
                                    OOQL.CreateConstants(string.Empty, GeneralDBType.String, "language "),
                //OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE", "department_no"),//20161215 mark by liwei1 for B001-161215015 
                //OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_NAME", "department_name")//20161215 mark by liwei1 for B001-161215015 
                //20161215 add by liwei1 for B001-161215015 ===begin===
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE"),
                                            OOQL.CreateConstants(string.Empty), "department_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_NAME"),
                                            OOQL.CreateConstants(string.Empty), "department_name"),
                //20161215 add by liwei1 for B001-161215015 ===begin===
                                    OOQL.CreateConstants(paraList[0], "manage_barcode_inventory")  //20161216 add by shenbao for P001-161215001//20170331 modi by wangyq for P001-170327001 old:manageBarCode
                                    , OOQL.CreateConstants(paraList[1], "warehouse_separator")//20170331 add by wangyq for P001-170327001
                                    , OOQL.CreateConstants(paraList[2], "feature")//20170331 add by wangyq for P001-170327001
                                )
                                .From("USER", "USER")
                                .InnerJoin("EMPLOYEE", "EMPLOYEE")
                                .On((OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID") == OOQL.CreateProperty("USER.EMPLOYEE_ID")))
                                .LeftJoin("EMPLOYEE.EMPLOYEE_D", "EMPLOYEE_D")//20161215 modi by liwei1 for B001-161215015 old:InnerJoin
                                .On((OOQL.CreateProperty("EMPLOYEE_D.EMPLOYEE_ID") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"))
                                    & (OOQL.CreateProperty("EMPLOYEE_D.IS_PRIMARY") == OOQL.CreateConstants(true, GeneralDBType.Boolean)))
                                .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")//20161215 modi by liwei1 for B001-161215015 old:InnerJoin
                                .On((OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID") == OOQL.CreateProperty("EMPLOYEE_D.ADMIN_UNIT_ID")))
                                .InnerJoin("USER.USER_ORG", "USER_ORG")
                                .On((OOQL.CreateProperty("USER_ORG.USER_ID") == OOQL.CreateProperty("USER.USER_ID")))
                                .InnerJoin("PLANT", "PLANT")
                                .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("USER_ORG.ORG.ROid")))
                                .Where((OOQL.AuthFilter("USER", "USER"))
                                    & ((OOQL.CreateProperty("USER.LOGONNAME") == OOQL.CreateConstants(logonName))
                                    & (OOQL.CreateProperty("USER_ORG.ORG.RTK") == OOQL.CreateConstants("PLANT"))));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string[] GetParaFIL() {
            ICreateService createService = this.GetService<ICreateService>("PARA_FIL");
            DependencyObject filEntity = createService.Create() as DependencyObject;
            string[] result = new string[] { string.Empty, string.Empty, string.Empty };
            if (filEntity.DependencyObjectType.Properties.Contains("BARCODE_SYMBOL_FOR_WH_BIN")) {
                QueryNode node = OOQL.Select(OOQL.CreateProperty("PARA_FIL.BC_INVENTORY_MANAGEMENT"),
                                           OOQL.CreateProperty("PARA_FIL.BARCODE_SYMBOL_FOR_WH_BIN"),
                                           OOQL.CreateProperty("PARA_FIL.ITEM_FEATURE_CONTROL"))
                                    .From("PARA_FIL", "PARA_FIL")
                                    .Where(OOQL.AuthFilter("PARA_FIL", "PARA_FIL"));
                DependencyObjectCollection filColl = this.GetService<IQueryService>().ExecuteDependencyObject(node);
                foreach (DependencyObject filObj in filColl) {
                    result[0] = filObj["BC_INVENTORY_MANAGEMENT"].ToBoolean() ? "Y" : "N";
                    if (string.IsNullOrEmpty(filObj["BARCODE_SYMBOL_FOR_WH_BIN"].ToStringExtension())) {
                        result[1] = "@";
                    } else {
                        result[1] = filObj["BARCODE_SYMBOL_FOR_WH_BIN"].ToStringExtension();
                    }
                    result[2] = filObj["ITEM_FEATURE_CONTROL"].ToBoolean() ? "Y" : "N";
                    break;
                }
            } else {
                bool isInvertory = UtilsClass.IsBCInventoryManagement(this.GetService<IQueryService>());
                result[0] = isInvertory ? "Y" : "N";
                result[1] = "@";
                result[2] = "Y";
            }
            return result;
        }
    }
}
