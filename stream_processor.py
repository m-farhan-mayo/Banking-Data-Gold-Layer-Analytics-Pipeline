import io
import pandas as pd
import json

class StreamProcessorRaw:

    # -------------------------------------------
    # CUSTOMER STREAM (JSON messy)
    # -------------------------------------------
    def process_customer(self, message):
        try:
            # Return raw JSON string as-is
            return {"raw_data": message}
        except Exception as e:
            print(f"❌ Customer processing error: {e}")
            return {"raw_data": message}

    # -------------------------------------------
    # ACCOUNT STREAM (CSV messy)
    # -------------------------------------------
    def process_account(self, message):
        try:
            # Return CSV row as-is
            return {"raw_data": message}
        except Exception as e:
            print(f"❌ Account processing error: {e}")
            return {"raw_data": message}

    # -------------------------------------------
    # TRANSACTION STREAM (PARQUET messy)
    # -------------------------------------------
    def process_transaction(self, message_bytes):
        try:
            buffer = io.BytesIO(message_bytes)
            df = pd.read_parquet(buffer)
            # Return each row as raw dict
            return [row.to_dict() for _, row in df.iterrows()]
        except Exception as e:
            print(f"❌ Transaction processing error: {e}")
            return [{"raw_data": message_bytes}]

    # -------------------------------------------
    # BRANCH STREAM (Mixed JSON / CSV)
    # -------------------------------------------
    def process_branch(self, message):
        try:
            # Return raw message as-is
            return {"raw_data": message}
        except Exception as e:
            print(f"❌ Branch processing error: {e}")
            return {"raw_data": message}
