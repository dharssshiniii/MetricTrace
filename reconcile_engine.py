import uuid
import pandas as pd
import mysql.connector

def run_reconciliation():
    # 1. Database Connection
    conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',  # <--- Update your password
        database='metrictrace_db'
    )

    # 2. Track processed items (Prevents duplicate logs across execution batches)
    already_audited_df = pd.read_sql("SELECT DISTINCT order_id FROM reconciliation_audit WHERE order_id IS NOT NULL;", conn)
    already_audited_orders = set(already_audited_df['order_id'])

    # 3. Read live variance view
    df = pd.read_sql("SELECT * FROM v_raw_revenue_variance;", conn)

    # 4. Filter for unseen data
    unseen_df = df[~df['sales_order_id'].isin(already_audited_orders)]

    if unseen_df.empty:
        print("⚡ [MetricTrace Engine] No new unseen records found. System is 100% reconciled.")
        conn.close()
        return

    batch_id = f"BATCH-{uuid.uuid4().hex[:8].upper()}"
    audit_rows = []

    # 5. Dynamic Root-Cause Rules Engine (Works for ANY dataset)
    for index, row in unseen_df.iterrows():
        order_id = row['sales_order_id']
        invoice_id = row['finance_invoice_id']
        sales_amt = float(row['sales_net_amount'] or 0.0)
        finance_amt = float(row['finance_net_amount'] or 0.0)
        variance = float(row['raw_variance_amount'] or 0.0)
        status = str(row['sales_status']).upper()

        # Skip perfectly matched transactions
        if variance == 0.0 and status in ['COMPLETED', 'SHIPPED', 'DELIVERED']:
            continue

        # RULE TIER 1: Unbilled / Revenue Leakage (Sales realized, Finance unbilled)
        if (pd.isna(invoice_id) or invoice_id == 'null') and status in ['COMPLETED', 'SHIPPED', 'DELIVERED']:
            tier = 1
            root_cause = "UNBILLED_ORDER"
            sub_cat = "MISSING_FINANCE_INVOICE"

        # RULE TIER 2: Filter Context & Pipeline Drift (Cancelled, Pending, Returned)
        elif status not in ['COMPLETED', 'SHIPPED', 'DELIVERED']:
            tier = 2
            root_cause = "FILTER_CONTEXT_DRIFT"
            sub_cat = f"STATUS_{status}"

        # RULE TIER 3: Cutoff, Timing, or Price Discrepancy
        else:
            tier = 3
            root_cause = "GRAIN_CUTOFF_MISMATCH"
            sub_cat = "TIMING_OR_PRICE_DISCREPANCY"

        audit_rows.append((
            batch_id, order_id, invoice_id if pd.notna(invoice_id) else None, 
            sales_amt, finance_amt, variance, root_cause, sub_cat, tier
        ))

    # 6. Bulk Insert
    if audit_rows:
        insert_sql = """
            INSERT INTO reconciliation_audit 
            (execution_batch_id, order_id, invoice_id, source_system_amount, target_system_amount, variance_amount, primary_root_cause, sub_category, reconciliation_tier)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        cursor = conn.cursor()
        cursor.executemany(insert_sql, audit_rows)
        conn.commit()
        cursor.close()
        print(f"✅ [MetricTrace Engine] Batch '{batch_id}' successfully executed! Processed {len(audit_rows)} unseen discrepancy records.")
    else:
        print("⚡ [MetricTrace Engine] All incoming records match perfectly. Zero variances detected.")

    conn.close()

if __name__ == "__main__":
    run_reconciliation()