//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/08 13:19:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>公共服务类</description>
//----------------------------------------------------------------  
//20161216 modi by shenbao for P001-161215001
//20170328 modi by wangyq for P001-170327001

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Digiwin.Common.Torridity;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using Digiwin.Common.Torridity.Metadata;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    public class UtilsClass {

        public static string SpaceValue { get { return " "; } }//20170328 add by wangyq for P001-170327001

        #region DataTable操作

        /// <summary>
        /// 创建数据表
        /// </summary>
        /// <param name="name">表名</param>
        /// <param name="columns">列集合</param>
        /// <returns></returns>
        public static DataTable CreateDataTable(string name, DataColumn[] columns) {
            DataTable table = new DataTable(name);
            table.Columns.AddRange(columns);

            return table;
        }

        /// <summary>
        /// 创建数据表
        /// </summary>
        /// <param name="name">表名</param>
        /// <param name="columnNames">列名称集合</param>
        /// <param name="columnTypes">列类型集合</param>
        /// <returns></returns>
        public static DataTable CreateDataTable(string name, string[] columnNames, Type[] columnTypes) {
            if (columnNames.Length != columnTypes.Length)
                throw new ArgumentException("columnNames,columnTypes");

            DataColumn[] columns = new DataColumn[columnNames.Length];
            for (int index = 0; index < columnNames.Length; index++)
                columns[index] = new DataColumn(columnNames[index], columnTypes[index]);

            return CreateDataTable(name, columns);
        }

        #endregion

        #region BulkCopy

        /// <summary>
        /// 创建BulkCopy对应关系
        /// </summary>
        /// <param name="map">mapping字典</param>
        /// <returns></returns>
        public static List<BulkCopyColumnMapping> CreateBulkMapping(Dictionary<string, string> map) {
            List<BulkCopyColumnMapping> bulkCopyColumnMapping = new List<BulkCopyColumnMapping>();
            foreach (string item in map.Keys) {
                bulkCopyColumnMapping.Add(new BulkCopyColumnMapping(item, map[item]));
            }

            return bulkCopyColumnMapping;
        }

        #endregion

        #region 插入数据到数据库或者临时表

        /// <summary>
        /// 插入数据
        /// 常量插入到数据库
        /// </summary>
        /// <param name="querySrv">查询服务</param>
        /// <param name="typeKeyPath">实体名称路径</param>
        /// <param name="dicPropertyValues">值</param>
        public static int InsertDocumentToDataBaseOrTmpTable(IQueryService querySrv, string typeKeyPath, Dictionary<string, QueryProperty> dicPropertyValues) {
            return InsertDocumentToDataBaseOrTmpTable(querySrv, typeKeyPath, dicPropertyValues, false);
        }

        /// <summary>
        /// 插入数据
        /// 常量插入
        /// </summary>
        /// <param name="querySrv">查询服务</param>
        /// <param name="typeKeyPath">实体名称路径</param>
        /// <param name="dicPropertyValues">值</param>
        /// <param name="isTmpTable">是否是临时表</param>
        public static int InsertDocumentToDataBaseOrTmpTable(IQueryService querySrv, string typeKeyPath, Dictionary<string, QueryProperty> dicPropertyValues, bool isTmpTable) {
            QueryNode insertNode = OOQL.Insert(typeKeyPath, dicPropertyValues.Keys).Values(dicPropertyValues.Values);
            if (!isTmpTable)
                return querySrv.ExecuteNoQueryWithManageProperties(insertNode);
            else
                return querySrv.ExecuteNoQuery(insertNode);
        }

        /// <summary>
        /// 插入数据
        /// 根据查询node批量插入
        /// </summary>
        /// <param name="querySrv">查询服务</param>
        /// <param name="typeKeyPath">实体名称路径</param>
        /// <param name="node">节点</param>
        /// <param name="isTmpTable">是否是临时表</param>
        public static int InsertDocumentToDataBaseOrTmpTable(IQueryService querySrv, string typeKeyPath, QueryNode node, string[] properties, bool isTmpTable) {
            QueryNode insertNode = OOQL.Insert(typeKeyPath, node, properties);
            try {
                if (!isTmpTable)
                    return querySrv.ExecuteNoQueryWithManageProperties(insertNode);
                else
                    return querySrv.ExecuteNoQuery(insertNode);
            } catch (Exception ex) {
                throw new CustomSqlException(ex.Message);
            }
        }

        /// <summary>
        /// 插入数据
        /// 根据查询node批量插入到数据库
        /// </summary>
        /// <param name="querySrv">查询服务</param>
        /// <param name="typeKeyPath">实体名称路径</param>
        /// <param name="node">节点</param>
        public static int InsertDocumentToDataBaseOrTmpTable(IQueryService querySrv, string typeKeyPath, QueryNode node, string[] properties) {
            return InsertDocumentToDataBaseOrTmpTable(querySrv, typeKeyPath, node, properties, false);
        }

        /// <summary>
        /// 修改实体或者临时表
        /// 捕获SQL类型异常
        /// </summary>
        /// <param name="qrySrv">查询服务</param>
        /// <param name="node">修改node</param>
        /// <param name="isTmpTable">是否为临时表</param>
        /// <returns></returns>
        public static int ExecuteNoQuery(IQueryService qrySrv, QueryNode node, bool isTmpTable) {
            try {
                if (!isTmpTable)
                    return qrySrv.ExecuteNoQueryWithManageProperties(node);
                else
                    return qrySrv.ExecuteNoQuery(node);
            } catch (Exception ex) {
                throw new CustomSqlException(ex.Message);
            }
        }

        /// <summary>
        /// 查询实体或者临时表
        /// 捕获SQL类型异常
        /// </summary>
        /// <param name="qrySrv">查询服务</param>
        /// <param name="node">查询node</param>
        /// <returns></returns>
        public static DependencyObjectCollection ExecuteDependencyObject(IQueryService qrySrv, QueryNode node) {
            try {
                return qrySrv.ExecuteDependencyObject(node);
            } catch (Exception ex) {
                throw new CustomSqlException(ex.Message);
            }
        }

        #endregion

        #region 获取单号

        /// <summary>
        /// 获取下一个单号
        /// </summary>
        /// <param name="documentNumberGenSrv">单号生成服务</param>
        /// <param name="docNo">当前单号,如果传入单号为空，则会调用单号生成服务获取单号</param>
        /// <param name="docID">单据类型</param>
        /// <param name="sequenceDigit">单据类型流水号位数</param>
        /// <param name="date">单据日期</param>
        /// <returns></returns>
        public static string NextNumber(IDocumentNumberGenerateService documentNumberGenSrv, string docNo
            , object docID, int sequenceDigit, DateTime date) {
            if (docNo == string.Empty)
                docNo = documentNumberGenSrv.NextNumber(docID, date);
            else {
                if (docNo.Length > sequenceDigit) {
                    docNo = docNo.Substring(0, docNo.Length - sequenceDigit)
                        + (docNo.Substring(docNo.Length - sequenceDigit, sequenceDigit).ToInt32() + 1).ToStringExtension().PadLeft(sequenceDigit, '0');
                }
            }

            return docNo;
        }

        #endregion

        #region 类型转换

        /// <summary>
        /// 日期类型参数
        /// </summary>
        private List<string> _dateTimeProperties = new List<string> { "report_datetime", "batch_processing_time", "last_transaction_date", "picking_date" };
        public List<string> DateTimeProperties {
            get { return _dateTimeProperties; }
            set { _dateTimeProperties = value; }
        }

        /// <summary>
        /// 数字类型参数
        /// </summary>
        private List<string> _decimalProperties = new List<string>() { "picking_qty", "objective_qty", "conversion_qty", "source_qty", "qty" };
        public List<string> DecimalProperties {
            get { return _decimalProperties; }
            set { _decimalProperties = value; }
        }

        /// <summary>
        /// 整数类型
        /// </summary>
        private List<string> _intProperties = new List<string>() { "seq" };
        public List<string> IntProperties {
            get { return _intProperties; }
            set { _intProperties = value; }
        }

        /// <summary>
        /// 将移动传过来的实体的字符串类型统一转换为对应的具体类型
        /// 如：字符串的 2016/11/11 17:40:22 转换为对应的日期类型
        /// 后续业务就不用额外的转换了
        /// </summary>
        /// <param name="sourceColls">原集合</param>
        /// <param name="businessTypeService"></param>
        /// <returns>返回转换类型之后的新集合</returns>
        public DependencyObjectCollection ConvertToDependencyObjectCollection(DependencyObjectCollection sourceColls, IBusinessTypeService businessTypeService) {
            //组织新的数据类型
            DependencyObjectType newType = ConvertToDependencyObjectType(sourceColls.ItemDependencyObjectType, businessTypeService);
            BuildCollectionPorpertyType(sourceColls.ItemDependencyObjectType, newType, businessTypeService);

            //新集合
            DependencyObjectCollection newColl = new DependencyObjectCollection(newType);
            BuildCollectionValue(sourceColls, newColl);

            return newColl;
        }

        /// <summary>
        /// 组织新的数据类型
        /// </summary>
        /// <param name="sourceType">原数据类型</param>
        /// <param name="newType">新数据类型</param>
        /// <param name="businessTypeService"></param>
        public void BuildCollectionPorpertyType(IDataEntityType sourceType, IDataEntityType newType, IBusinessTypeService businessTypeService) {
            //集合属性需重新运用递归计算类型
            foreach (var subCollPro in sourceType.CollectionProperties) {
                DependencyObjectType subNewType = ConvertToDependencyObjectType(subCollPro.ItemDataEntityType, businessTypeService);
                (newType as DependencyObjectType).RegisterCollectionProperty(subCollPro.Name, subNewType);
                BuildCollectionPorpertyType(subCollPro.ItemDataEntityType, subNewType, businessTypeService);
            }
        }

        /// <summary>
        /// 简单属性的类型转换
        /// 将移动对应的实体的字符串类型统一转换为指定的数据类型
        /// </summary>
        /// <param name="sourceType">原数据类型</param>
        /// <param name="businessTypeService">业务服务接口</param>
        /// <returns></returns>
        public DependencyObjectType ConvertToDependencyObjectType(IDataEntityType sourceType, IBusinessTypeService businessTypeService) {
            DependencyObjectType newType = new DependencyObjectType(sourceType.Name);
            foreach (DependencyProperty property in sourceType.SimpleProperties) {
                if (_dateTimeProperties.Contains(property.Name)) {
                    newType.RegisterSimpleProperty(property.Name, typeof(DateTime), OrmDataOption.EmptyDateTime
                        , false, new Attribute[] { new SimplePropertyAttribute(GeneralDBType.DateTime) });
                } else if (_decimalProperties.Contains(property.Name)) {
                    newType.RegisterSimpleProperty(property.Name, businessTypeService.SimpleQuantityType, 0m
                        , false, new Attribute[] { businessTypeService.SimpleQuantity });
                } else if (_intProperties.Contains(property.Name)) {
                    newType.RegisterSimpleProperty(property.Name, typeof(int), 0
                        , false, new Attribute[] { new SimplePropertyAttribute(GeneralDBType.Int32) });
                } else {
                    newType.RegisterSimpleProperty(property.Name, typeof(string), string.Empty
                        , false, new Attribute[] { new SimplePropertyAttribute(GeneralDBType.String) });
                }
            }

            return newType;
        }

        /// <summary>
        /// 组织新的集合
        /// </summary>
        /// <param name="sourceCollection"></param>
        /// <param name="newCollection"></param>
        public void BuildCollectionValue(DependencyObjectCollection sourceCollection, DependencyObjectCollection newCollection) {
            foreach (DependencyObject subObj in sourceCollection) {
                DependencyObject newObj = newCollection.AddNew();
                //简单属性赋值
                foreach (DependencyProperty property in (sourceCollection.ItemDependencyObjectType as IDataEntityType).SimpleProperties) {
                    if (_dateTimeProperties.Contains(property.Name)) {
                        newObj[property.Name] = subObj[property.Name].ToDate();
                    } else if (_decimalProperties.Contains(property.Name)) {
                        newObj[property.Name] = subObj[property.Name].ToDecimal();
                    } else {
                        newObj[property.Name] = subObj[property.Name].ToStringExtension();
                    }
                }

                //集合属性赋值
                foreach (DependencyProperty property in (sourceCollection.ItemDependencyObjectType as IDataEntityType).CollectionProperties) {
                    DependencyObjectCollection subColl = property.GetValue(subObj) as DependencyObjectCollection;
                    DependencyObjectCollection newC = newObj[property.Name] as DependencyObjectCollection;
                    BuildCollectionValue(subColl, newC);
                }
            }
        }

        #endregion

        #region 业务相关方法

        /// <summary>
        /// 创建服务返回集合
        /// </summary>
        /// <returns></returns>
        public DependencyObjectCollection CreateReturnCollection() {
            DependencyObjectType type = new DependencyObjectType("ReturnCollection");
            type.RegisterSimpleProperty("doc_no", typeof(string));

            DependencyObjectCollection collDepObj = new DependencyObjectCollection(type);

            return collDepObj;
        }

        /// <summary>
        /// 异常服务返回集合
        /// </summary>
        /// <returns></returns>
        public static DependencyObjectCollection CreateExceptionReturnCollection() {
            DependencyObjectType type = new DependencyObjectType("ReturnCollection");
            type.RegisterSimpleProperty("code", typeof(int));
            type.RegisterSimpleProperty("sql_code", typeof(string));
            type.RegisterSimpleProperty("description", typeof(string));
            type.RegisterSimpleProperty("report_no", typeof(string));

            DependencyObjectCollection Rtn = new DependencyObjectCollection(type);

            return Rtn;
        }

        /// <summary>
        /// 设置异常服务返回值
        /// </summary>
        /// <param name="coll"></param>
        /// <param name="code"></param>
        /// <param name="sqlCode"></param>
        /// <param name="description"></param>
        /// <param name="reportNo"></param>
        public static void SetValue(DependencyObjectCollection coll, int code, string sqlCode, string description, string reportNo) {
            DependencyObject obj = coll.AddNew();
            obj["code"] = code;
            obj["sql_code"] = sqlCode;
            obj["description"] = description;
            obj["report_no"] = reportNo;
        }

        /// <summary>
        /// 查询是否启用条码交易明细管理
        /// </summary>
        /// <param name="srv"></param>
        /// <returns></returns>
        public static bool IsBCLineManagement(IQueryService srv) {
            QueryNode node = OOQL.Select("PARA_FIL.BC_LINE_MANAGEMENT")
                .From("PARA_FIL", "PARA_FIL")
                .Where(OOQL.AuthFilter("PARA_FIL", "PARA_FIL"));

            object obj = srv.ExecuteScalar(node);
            if (obj == null)
                return false;

            return obj.ToBoolean();
        }

        //20161216 add by shenbao for P001-161215001
        /// <summary>
        /// 查询是否启用启用条码库存管理
        /// </summary>
        /// <param name="srv"></param>
        /// <returns></returns>
        public static bool IsBCInventoryManagement(IQueryService srv) {
            QueryNode node = OOQL.Select("PARA_FIL.BC_INVENTORY_MANAGEMENT")
                .From("PARA_FIL", "PARA_FIL")
                .Where(OOQL.AuthFilter("PARA_FIL", "PARA_FIL"));

            object obj = srv.ExecuteScalar(node);
            if (obj == null)
                return false;

            return obj.ToBoolean();
        }

        //20170328 add by wangyq for P001-170327001
        /// <summary>
        /// 
        /// </summary>
        /// <param name="group">原来查询的条件,必须传入，避免拼接半天太麻烦</param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public static QueryConditionGroup CreateNewConditionByParameter(QueryConditionGroup group, DependencyObjectCollection condition, ConditionPropertyNameEntity conPropertyEntity) {
            if (condition.ItemDependencyObjectType.Properties.Contains("seq")
                && condition.ItemDependencyObjectType.Properties.Contains("value")) {//都没有设置的时候会没有传入这个字段
                foreach (DependencyObject conObj in condition) {
                    string leftName = string.Empty;
                    bool isEmptyValue;
                    if (conObj["seq"].ToStringExtension() == "2") {//日期类型要转换为日期后判断
                        isEmptyValue = Maths.IsEmpty(conObj["value"].ToDate());
                    } else {
                        isEmptyValue = Maths.IsEmpty(conObj["value"]);
                    }
                    if (!isEmptyValue && conPropertyEntity.ContainProperty.Length > 0 && conPropertyEntity.ContainProperty.Contains(conObj["seq"].ToStringExtension())) {
                        switch (conObj["seq"].ToStringExtension()) {
                            case "1"://单号
                                leftName = conPropertyEntity.DocNoName;
                                break;
                            case "2"://日期
                                group &= OOQL.CreateProperty(conPropertyEntity.DocDateName) >= OOQL.CreateConstants(conObj["value"].ToDate(), GeneralDBType.Date);
                                break;
                            case "3"://人员
                                leftName = conPropertyEntity.EmployeeName;
                                break;
                            case "4"://部门
                                leftName = conPropertyEntity.AdminUnitName;
                                break;
                            case "5"://供应商
                                leftName = conPropertyEntity.SupplierName;
                                break;
                            case "6"://客户
                                leftName = conPropertyEntity.CustomerName;
                                break;
                            default:
                                break;
                        }
                        if (!string.IsNullOrEmpty(leftName)) {
                            group &= OOQL.CreateProperty(leftName).Like(OOQL.CreateConstants("%" + conObj["value"].ToString() + "%", GeneralDBType.String));
                        }
                    }
                }
            }
            return group;
        }

        #endregion

    }

    /// <summary>
    /// 自定义SQL类型异常
    /// 由于SqlException无法继承和实例化，而且执行OOQL抛出的异常又是BusinessRuleException类型的，导致无法精确匹配sql类型异常
    /// 我们可以捕捉OOQL的异常，再抛出CustomSqlException，后续捕捉即可
    /// </summary>
    public class CustomSqlException : Exception {
        public CustomSqlException(string message) : base(message) { }

        public override string Message {
            get {
                return base.Message;
            }
        }
    }

    //20170328 add by wangyq for P001-170327001
    /// <summary>
    /// CreateNewConditionByParameter方法里面拼接条件的左边字段名称集合
    /// </summary>
    public class ConditionPropertyNameEntity {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="docNoName">单号</param>
        /// <param name="docDateName">日期</param>
        /// <param name="ContainProperty">需要哪几个条件,必传</param>
        public ConditionPropertyNameEntity(string docNoName, string docDateName, string[] containProperty) {
            DocNoName = docNoName;
            DocDateName = docDateName;
            EmployeeName = "EMPLOYEE.EMPLOYEE_CODE";
            AdminUnitName = "ADMIN_UNIT.ADMIN_UNIT_CODE";
            SupplierName = "SUPPLIER.SUPPLIER_CODE";
            CustomerName = "CUSTOMER.CUSTOMER_CODE";
            ContainProperty = containProperty;
        }
        /// <summary>
        /// 单号
        /// </summary>
        public string DocNoName { get; set; }
        /// <summary>
        /// 日期
        /// </summary>
        public string DocDateName { get; set; }
        /// <summary>
        /// 人员
        /// </summary>
        public string EmployeeName { get; set; }
        /// <summary>
        /// 部门
        /// </summary>
        public string AdminUnitName { get; set; }
        /// <summary>
        /// 供货商
        /// </summary>
        public string SupplierName { get; set; }
        /// <summary>
        /// 客户
        /// </summary>
        public string CustomerName { get; set; }
        /// <summary>
        /// 不同的实体调用需要不同的条件
        /// </summary>
        public string[] ContainProperty { get; set; }
    }
}