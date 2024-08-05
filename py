import snowflake.connector
from snowflake.connector import ProgrammingError

# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user='YOUR_USER',
    password='YOUR_PASSWORD',
    account='YOUR_ACCOUNT',
    warehouse='YOUR_WAREHOUSE',
    database='YOUR_DATABASE',
    schema='YOUR_SCHEMA'
)

def create_procedure(conn):
    try:
        sql_command = """
        CREATE OR REPLACE PROCEDURE SP_PSD(
            ProcessCenter_ID INT,
            DaysCleared INT,
            Office_ID INT = NULL
        )
        RETURNS STRING
        LANGUAGE PYTHON
        EXECUTE AS CALLER
        AS
        $$
        import snowflake.connector
        import datetime
        
        def run_procedure(ProcessCenter_ID, DaysCleared, Office_ID):
            conn = snowflake.connector.connect(
                user=context['user'],
                password=context['password'],
                account=context['account']
            )
            cursor = conn.cursor()
            clientOrderID, orderDetailID, relatedClientOrderID, relatedOrderDetailID, orderTypeID, keyID, keyType = None, None, None, None, None, None, None
            settlementMethodID, isAggregation = None, None
            officeIDMalta = 60
            opsStatusListDeleted = 813
            postDate, ordered = None, None

            try:
                cursor.execute("DROP TABLE IF EXISTS #cursor")
                cursor.execute("DROP TABLE IF EXISTS #items")

                cursor.execute("""
                CREATE TEMPORARY TABLE #cursor (
                    ClientOrder_ID INT,
                    OrderDetail_ID INT,
                    RelatedClientOrder_ID INT,
                    RelatedOrderDetail_ID INT,
                    OrderType_ID INT,
                    Ordered TIMESTAMP_NTZ,
                    IsAggregation BOOLEAN
                )
                """)

                cursor.execute("""
                CREATE TEMPORARY TABLE #items (
                    ClientOrder_ID INT,
                    OrderDetail_ID INT,
                    RelatedClientOrder_ID INT,
                    RelatedOrderDetail_ID INT,
                    OrderType_ID INT,
                    Account STRING,
                    ConfirmationNo STRING,
                    ItemNo INT,
                    Ordered TIMESTAMP_NTZ,
                    ARPostDate TIMESTAMP_NTZ,
                    SettlementMethod_ID INT,
                    SettlementMethod STRING,
                    SettlementCurrencyCode STRING,
                    SettlementAmount FLOAT,
                    ItemType_ID INT,
                    ItemTypeDescription STRING,
                    SendDate TIMESTAMP_NTZ,
                    ValueDate TIMESTAMP_NTZ,
                    ClearDate TIMESTAMP_NTZ,
                    CurrencyCode STRING,
                    ForeignAmount FLOAT,
                    FundedBy INT,
                    FundedByDescription STRING,
                    IsAggregation BOOLEAN,
                    OurCheckRef STRING
                )
                """)

                insert_items_sql = f"""
                INSERT INTO #items
                SELECT 
                    h.ClientOrder_ID,
                    d.OrderDetail_ID,
                    h.RelatedClientOrder_ID,
                    D.RelatedOrderDetail_ID,
                    h.OrderType_ID,
                    c.Account,
                    h.ConfirmationNo,
                    d.ItemNo,
                    h.Ordered,
                    NULL, -- ARPostDate
                    NULL, -- SettlementMethod_ID
                    NULL, -- SettlementMethod
                    h.Settlement_SWIFT, -- SettlementCurrencyCode
                    d.Extension, -- SettlementAmount
                    d.ItemType_ID,
                    d.ItemTypeDescription,
                    COALESCE(CASE o.status_id WHEN 776 THEN o.StatusUpdated ELSE NULL END,
                             CASE oa.status_id WHEN 776 THEN oa.StatusUpdated ELSE NULL END), -- SendDate
                    COALESCE(o.ValueDate, oa.ValueDate),
                    r.ClearDate,
                    d.CurrencyCode,
                    d.ForeignAmount,
                    d.FundedBy,
                    REPLACE(p.Description, 'Payment Funded By - ', ''),
                    NVL(ex.IsAggregation, FALSE),
                    d.OurCheckRef
                FROM 
                    client c 
                    JOIN TrRawHeader h ON c.Client_ID = h.Client_ID
                    JOIN TrRawDetail d ON h.ClientOrder_ID = d.ClientOrder_ID
                    LEFT JOIN ClientOrder_Extended_History ex ON h.ClientOrder_ID = ex.ClientOrder_ID
                    LEFT JOIN Reconciliation_History r ON d.OrderDetail_ID = r.OrderDetail_ID
                    LEFT JOIN OpsLog o ON d.OrderDetail_ID = o.OrderDetail_ID
                    LEFT JOIN OpsLog_Archive oa ON d.OrderDetail_ID = oa.OrderDetail_ID
                    LEFT JOIN PickListItem p ON d.FundedBy = p.PickListItem_ID
                WHERE 
                    c.ProcessCenter_ID = {ProcessCenter_ID}
                    AND c.Office_ID = COALESCE(NULLIF({Office_ID}, -1), c.Office_ID)
                    AND h.OrderType_ID IN (1, 4, 9, 13, 103)
                    AND d.ItemType_ID <> 117
                    AND (c.Office_ID != 13 OR d.ItemType_ID != 3)
                    AND NOT (d.Extension = 0 AND d.ForeignAmount = 0)
                    AND c.Status_ID IN (3, 4)
                    AND COALESCE(r.ClearDate, CURRENT_TIMESTAMP) > DATEADD(day, -{DaysCleared}, CURRENT_TIMESTAMP)
                    AND (
                        (c.Office_ID = {officeIDMalta} AND YEAR(h.Ordered) IN (YEAR(CURRENT_DATE), YEAR(CURRENT_DATE) - 1, YEAR(CURRENT_DATE) - 2))
                        OR
                        (c.Office_ID != {officeIDMalta} AND YEAR(h.Ordered) IN (YEAR(CURRENT_DATE), YEAR(CURRENT_DATE) - 1))
                    )
                """
                cursor.execute(insert_items_sql)

                cursor.execute("DELETE FROM #items WHERE ClientOrder_ID IN (SELECT PreviousClientOrder_ID FROM RSRepurchase)")
                cursor.execute("DELETE FROM #items WHERE OurCheckRef = 'No new EFT'")
                cursor.execute(f"DELETE FROM #items WHERE OrderDetail_ID IN (SELECT OrderDetail_ID FROM OpsLog_Archive WHERE Status_ID = {opsStatusListDeleted})")

                cursor.execute("""
                INSERT INTO #cursor
                SELECT
                    ClientOrder_ID,
                    OrderDetail_ID,
                    RelatedClientOrder_ID, 
                    RelatedOrderDetail_ID,
                    OrderType_ID,
                    Ordered,
                    IsAggregation
                FROM #items i
                WHERE EXISTS (
                    SELECT 1
                    FROM ARTransaction AS AR
                    WHERE AR.ClientOrder_ID IN (i.ClientOrder_ID, i.RelatedClientOrder_ID)
                )
                """)

                result = cursor.execute("SELECT TOP 1 ClientOrder_ID, OrderDetail_ID, RelatedClientOrder_ID, RelatedOrderDetail_ID, OrderType_ID, Ordered, IsAggregation FROM #cursor").fetchone()
                if result:
                    clientOrderID, orderDetailID, relatedClientOrderID, relatedOrderDetailID, orderTypeID, ordered, isAggregation = result

                while result:
                    keyID = None
                    keyType = None

                    if orderTypeID in (1, 13) or (orderTypeID == 4 and isAggregation):
                        keyID = clientOrderID
                        keyType = 0
                    elif orderTypeID == 9:
                        keyID = relatedOrderDetailID
                        keyType = 1
                    else:
                        keyID = orderDetailID
                        keyType = 1

                    postDate = None
                    settlementMethodID = None

                    if orderTypeID == 9:
                        result = cursor.execute(f"""
                        SELECT TOP 1 ar.LastUpdate_Date, ar.ActualPaymentMethod_ID
                        FROM ARTransaction ar
                        WHERE ar.Key_ID = {keyID}
                            AND ar.KeyType = {keyType}
                            AND ar.Type_ID <> 11
                            AND ar.ARBalanceDue_SC = 0
                            AND ar.LastUpdate_Date < '{ordered}'
                        ORDER BY ar.LastUpdate_Date DESC
                        """).fetchone()
                        if result:
                            postDate, settlementMethodID = result
                    else:
                        result = cursor.execute(f"""
                        SELECT TOP 1 ar.LastUpdate_Date, ar.ActualPaymentMethod_ID
                        FROM ARTransaction ar
                        WHERE ar.Key_ID = {keyID}
                            AND ar.KeyType = {keyType}
                            AND ar.Type_ID <> 11
                            AND ar.ARBalanceDue_SC = 0
                        ORDER BY ar.LastUpdate_Date DESC
                        """).fetchone()
                        if result:
                            postDate, settlementMethodID = result

                    if not settlementMethodID and result:
                        if orderTypeID == 9:
                            result = cursor.execute(f"""
                            SELECT TOP 1 ar.ActualPaymentMethod_ID
                            FROM ARTransaction ar
                            WHERE ar.Key_ID = {keyID}
                                AND ar.KeyType = {keyType}
                                AND ar.Type_ID <> 11
                                AND ar.ARBalanceDue_SC = 0
                                AND ar.ActualPaymentMethod_ID IS NOT NULL
                                AND ar.LastUpdate_Date < '{ordered}'
                            ORDER BY ar.LastUpdate_Date DESC
                            """).fetchone()
                            if result:
                                settlementMethodID = result[0]
                        else:
                            result = cursor.execute(f"""
                            SELECT TOP 1 ar.ActualPaymentMethod_ID
                            FROM ARTransaction ar
                            WHERE ar.Key_ID = {keyID}
                                AND ar.KeyType = {keyType}
                                AND ar.Type_ID <> 11
                                AND ar.ARBalanceDue_SC = 0
                                AND ar.ActualPaymentMethod_ID IS NOT NULL
                            ORDER BY ar.LastUpdate_Date DESC
                            """).fetchone()
                            if result:
                                settlementMethodID = result[0]

                    if not settlementMethodID and orderTypeID in (1, 13):
                        keyID = None

                        if relatedClientOrderID:
                            result = cursor.execute(f"""
                            SELECT RelatedClientOrder_ID
                            FROM TrRaw
