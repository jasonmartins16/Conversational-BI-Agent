# Final test module

import streamlit as st
import requests
import pandas as pd
import json
import os
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY") or st.secrets.get("GROQ_API_KEY")
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN") or st.secrets.get("MONDAY_API_TOKEN")
DEALS_BOARD_ID = os.getenv("DEALS_BOARD_ID") or st.secrets.get("DEALS_BOARD_ID")
WORK_ORDERS_BOARD_ID = os.getenv("WORK_ORDERS_BOARD_ID") or st.secrets.get("WORK_ORDERS_BOARD_ID")

client = OpenAI(
    api_key=GROQ_API_KEY,
    base_url="https://api.groq.com/openai/v1"
)

# ==========================================
# 1. MONDAY.COM API & DATA CLEANING
# ==========================================

def fetch_monday_data(board_id):
    """Hits the live monday.com API and fetches human-readable column titles."""
    url = "https://api.monday.com/v2"
    headers = {"Authorization": MONDAY_API_TOKEN, "Content-Type": "application/json"}
    
    # Updated GraphQL to explicitly ask for the column's title
    query = f"""
    {{
      boards(ids: {board_id}) {{
        items_page(limit: 100) {{
          items {{
            name
            column_values {{
              column {{
                title
              }}
              text
            }}
          }}
        }}
      }}
    }}
    """
    response = requests.post(url, headers=headers, json={"query": query})
    if response.status_code != 200:
        print(f"API Error: {response.text}") # Good for debugging in terminal
        return None
    
    data = response.json()
    items = data.get('data', {}).get('boards', [{}])[0].get('items_page', {}).get('items', [])
    
    parsed_data = []
    for item in items:
        row = {'Deal Name': item.get('name', 'Unknown')}
        for col in item.get('column_values', []):
            # Extract the human-readable title and make it lowercase so our Pandas code matches it perfectly
            col_title = col.get('column', {}).get('title', '').lower()
            col_text = col.get('text', '')
            if col_title:
                row[col_title] = col_text
        parsed_data.append(row)
        
    df = pd.DataFrame(parsed_data)
    
    # DEBUG TRICK: Print the columns to your terminal so you can verify them!
    print(f"--- Fetched Board {board_id} Columns ---")
    print(df.columns.tolist())
    
    return df

def get_deals_data(sector_filter=None, stage_filter=None):
    """Tool: Fetches, cleans, filters, and returns aggregated data + item names."""
    df = fetch_monday_data(DEALS_BOARD_ID)
    if df is None or df.empty:
        return "Error: Failed to fetch deals data."
    
    # Clean Data
    if 'masked deal value' in df.columns:
        df['masked deal value'] = df['masked deal value'].astype(str).str.replace(r'[^\d.]', '', regex=True)
        df['masked deal value'] = pd.to_numeric(df['masked deal value'], errors='coerce').fillna(0)
    
    df.fillna('Unknown', inplace=True)
    
    # Filter by Sector
    if sector_filter:
        sector_col = [c for c in df.columns if 'sector' in c.lower() or 'service' in c.lower()]
        if sector_col:
            df = df[df[sector_col[0]].astype(str).str.contains(sector_filter, case=False, na=False)]

    # Filter by Stage (NEW)
    if stage_filter:
        stage_col = [c for c in df.columns if 'stage' in c.lower() or 'status' in c.lower()]
        if stage_col:
            df = df[df[stage_col[0]].astype(str).str.contains(stage_filter, case=False, na=False)]

    # Math Aggregation
    total_deals = len(df)
    total_revenue = df['masked deal value'].sum() if 'masked deal value' in df.columns else 0
    
    # HYBRID RETURN: Get the actual names of the deals so the AI isn't blind, 
    # but limit it to 20 to strictly protect the token limit!
    deal_names = df['Deal Name'].head(20).tolist() if 'Deal Name' in df.columns else []

    summary = f"""
    BI METRICS SUMMARY:
    - Total Deals matching criteria: {total_deals}
    - Total Pipeline Value: ${total_revenue:,.2f}
    
    MATCHING DEAL NAMES (Top 20):
    {deal_names}
    """
    return summary

def get_work_orders_data():
    """Tool: Fetches, cleans, and aggregates Work Orders data."""
    df = fetch_monday_data(WORK_ORDERS_BOARD_ID)
    if df is None or df.empty:
        return "Error: Failed to fetch work orders data."
    
    df.fillna('Unknown', inplace=True)
    
    # Aggregate work orders
    total_orders = len(df)
    
    status_counts = {}
    status_col = [c for c in df.columns if 'status' in c.lower()]
    if status_col:
        status_counts = df[status_col[0]].value_counts().to_dict()
        
    summary = f"""
    BI METRICS SUMMARY:
    - Total Work Orders: {total_orders}
    - Status Breakdown: {status_counts}
    """
    return summary

# ==========================================
# 2. LLM TOOL DEFINITIONS
# ==========================================

tools = [
    {
        "type": "function",
        "function": {
            "name": "get_deals_data",
            "description": "Fetches Deals data. Use this for revenue, pipeline, or to find specific deal names.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sector_filter": {
                        "type": ["string", "null"], # <-- FIX: Tell Groq nulls are allowed
                        "description": "Optional sector filter (e.g., 'energy')."
                    },
                    "stage_filter": {
                        "type": ["string", "null"], # <-- FIX: Tell Groq nulls are allowed
                        "description": "Optional stage filter (e.g., 'H. Work Order Received')."
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_work_orders_data",
            "description": "Fetches Work Orders data.",
        }
    }
]

# ==========================================
# 3. STREAMLIT UI & CONVERSATION LOGIC
# ==========================================

st.set_page_config(page_title="Founder BI Agent", layout="wide")
st.title("📊 Monday.com BI Intelligence Agent")
st.markdown("Ask questions about revenue, pipeline health, or work order statuses.")

if "messages" not in st.session_state:
    st.session_state.messages = [
        {
            "role": "system", 
            "content": "You are a BI agent for company founders. Answer questions based ONLY on the data provided by your tools. If data is missing or messy, explicitly state that caveat to the founder. If the query is ambiguous, ask clarifying questions. IMPORTANT: You are integrated via a strict JSON tool-calling API. NEVER output raw <function> or XML tags in your text responses. Always use the native tool-calling schema."
        }
    ]

for msg in st.session_state.messages:
    # Safely handle both standard dictionaries and SDK objects
    role = msg["role"] if isinstance(msg, dict) else msg.role
    content = msg["content"] if isinstance(msg, dict) else msg.content
    
    # Only draw the message if it's not a background system/tool message AND it has text
    if role not in ["system", "tool"] and content is not None:
        with st.chat_message(role):
            st.markdown(content)

if prompt := st.chat_input("E.g., How is our pipeline looking for the energy sector?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        response_placeholder = st.empty()
        trace_container = st.container()
        
        try:
            # 1. Send to Groq
            response = client.chat.completions.create(
                model="openai/gpt-oss-20b",
                messages=st.session_state.messages,
                tools=tools,
                tool_choice="auto"
            )
            
            response_message = response.choices[0].message
            # SANITIZATION FIX: Strip out the hidden 'reasoning' attribute that crashes Groq
            clean_assistant_msg = {
                "role": response_message.role,
                "content": response_message.content,
            }
            
            if response_message.tool_calls:
                clean_assistant_msg["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": tc.type,
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments
                        }
                    } for tc in response_message.tool_calls
                ]
                
            st.session_state.messages.append(clean_assistant_msg)
            
            # 2. Check for Tool Call
            if response_message.tool_calls:
                with trace_container:
                    st.markdown("### 🔍 Agent Action Trace")
                    
                for tool_call in response_message.tool_calls:
                    function_name = tool_call.function.name
                    
                    # Parse the arguments the AI decided to pass (like sector="energy")
                    arguments = json.loads(tool_call.function.arguments) if tool_call.function.arguments else {}
                    
                    with trace_container:
                        with st.status(f"Executing live API call: `{function_name}`...", expanded=True) as status:
                            st.write(f"Connecting to monday.com API... (Args: {arguments})")
                            st.write("Fetching raw data...")
                            st.write("Applying Pandas data cleaning and formatting to CSV...")
                            
                            if function_name == "get_deals_data":
                                sector_filter = arguments.get("sector_filter")
                                stage_filter = arguments.get("stage_filter") # Catch the new filter
                                function_response = get_deals_data(sector_filter=sector_filter, stage_filter=stage_filter)
                            elif function_name == "get_work_orders_data":
                                function_response = get_work_orders_data()
                            else:
                                function_response = "Error: Unknown function"
                                
                            status.update(label=f"Successfully executed `{function_name}`", state="complete", expanded=False)

                    st.session_state.messages.append(
                        {
                            "tool_call_id": tool_call.id,
                            "role": "tool",
                            "name": function_name,
                            "content": function_response,
                        }
                    )
                
                # 3. Final Insight Generation
                messages_for_final = st.session_state.messages + [
                    {"role": "system", "content": "You have received the data. Provide the final BI insight to the user now. Do NOT call any more tools."}
                ]

                second_response = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    messages=messages_for_final,
                    tools=tools # FIX 1: Pass tools so the Groq backend doesn't crash
                )

                final_message = second_response.choices[0].message

                # FIX 2: Intercept the stubborn AI if it tries to call a tool a second time
                if final_message.tool_calls:
                    final_answer = "Here is the raw data summary I retrieved:\n\n" + str(function_response)
                else:
                    final_answer = final_message.content
                    
                response_placeholder.markdown(final_answer)
                st.session_state.messages.append({"role": "assistant", "content": final_answer})
                
            else:
                response_placeholder.markdown(response_message.content)
                
        except Exception as e:
            st.error(f"Agent encountered an error: {str(e)}")