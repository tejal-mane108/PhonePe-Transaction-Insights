import streamlit as st
import mysql.connector 
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
from urllib.request import urlopen

mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "root",
    database = "phonepe_insights",
    autocommit = True
)
mycursor = mydb.cursor()

def map_data(year, quater):
    sql = """SELECT
                state,
                SUM(transaction_amount) as Total_Transaction_Value
            FROM
                aggregated_transactions_stats
            WHERE
                year = %s AND quater = %s
            GROUP BY
                state 
            ORDER BY
                Total_Transaction_Value DESC;"""
    mycursor.execute(sql, (year, quater))
    data = mycursor.fetchall()
    df = pd.DataFrame(data, columns= ['state',"Total_Transaction_Value"])
    return df

def map_state(df):
    mapping = {
        'andaman-&-nicobar-islands':"Andaman & Nicobar",
        "andhra-pradesh":"Andhra Pradesh",
        "arunachal-pradesh":"Arunachal Pradesh",
        'assam':'Assam',
        'bihar':'Bihar',
        'chandigarh':'Chandigarh',
        'chhattisgarh':'Chhattisgarh',
        'dadra-&-nagar-haveli-&-daman-&-diu':'Dadra and Nagar and Daman and Diu',
        'delhi':'Delhi',
        'goa':'Goa',
        'gujarat':'Gujarat',
        'haryana':'Haryana',
        'himachal-pradesh':'Himachal Pradesh',
        'jammu-&-kashmir':'Jammu & Kashmir',
        'jharkhand':'Jharkhand',
        'karnataka':'Karnataka',
        'Kerala':'Kerala',
        'ladakh':'Ladakh',
        'madhya-pradesh':'Madhya Pradesh',
        'maharashtra':'Maharashtra',
        'manipur':'Manipur',
        'meghalaya':'Meghalaya',
        'mizoram':'Mizoram',
        'nagaland':'Nagaland',
        'odisha':'Odisha',
        'puducherry':'Puducherry',
        'punjab':'Punjab',
        'rajasthan':'Rajasthan',
        'sikkim':'Sikkin',
        'tamil-nadu':'Tamil Nadu',
        'telangana':'Telangana',
        'uttar-pradesh':'Uttar Pradesh',
        'uttarakhand':'Uttar Pradesh',
        'west-bengal':'West Bengal'
    }
    df['state'] = df['state'].map(mapping)
    return df

GEOJSON_URL = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"

# --------- helpers ----------
def _pyify(x):
    if isinstance(x, (np.integer,)):  return int(x)
    if isinstance(x, (np.floating,)): return float(x)
    if isinstance(x, (np.bool_,)):    return bool(x)
    return x

def fetch_df(sql, params=None, columns=None):
    safe_params = tuple(_pyify(p) for p in (params or ()))
    mycursor.execute(sql, safe_params)
    rows = mycursor.fetchall()
    return pd.DataFrame(rows, columns=columns)

def map_state_names(df, col:str = "state"):
    """Map Pulse raw state keys to pretty names; handle None/bytes/mixed dtypes safely."""
    mapping = {
        'andaman-&-nicobar-islands': "Andaman & Nicobar",
        'andhra-pradesh': "Andhra Pradesh",
        'arunachal-pradesh': "Arunachal Pradesh",
        'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
        'chhattisgarh': 'Chhattisgarh',
        'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra & Nagar Haveli and Daman & Diu',
        'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana',
        'himachal-pradesh': 'Himachal Pradesh', 'jammu-&-kashmir': 'Jammu & Kashmir',
        'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
        'ladakh': 'Ladakh', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
        'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram',
        'nagaland': 'Nagaland', 'odisha': 'Odisha', 'puducherry': 'Puducherry',
        'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim',
        'tamil-nadu': 'Tamil Nadu', 'telangana': 'Telangana',
        'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
        'west-bengal': 'West Bengal'
    }
    out = df.copy()
    s = out[col].apply(
        lambda x: x.decode() if isinstance(x, (bytes, bytearray)) else ("" if x is None else str(x))
    )
    s = s.str.strip().str.lower()
    out[col] = s.map(mapping)
    return out

INV_STATE = {
    "Andaman & Nicobar": 'andaman-&-nicobar-islands',
    "Andhra Pradesh": 'andhra-pradesh',
    "Arunachal Pradesh": 'arunachal-pradesh',
    "Assam": 'assam',
    "Bihar": 'bihar',
    "Chandigarh": 'chandigarh',
    "Chhattisgarh": 'chhattisgarh',
    "Dadra & Nagar Haveli and Daman & Diu": 'dadra-&-nagar-haveli-&-daman-&-diu',
    "Delhi": 'delhi',
    "Goa": 'goa',
    "Gujarat": 'gujarat',
    "Haryana": 'haryana',
    "Himachal Pradesh": 'himachal-pradesh',
    "Jammu & Kashmir": 'jammu-&-kashmir',
    "Jharkhand": 'jharkhand',
    "Karnataka": 'karnataka',
    "Kerala": 'kerala',
    "Ladakh": 'ladakh',
    "Madhya Pradesh": 'madhya-pradesh',
    "Maharashtra": 'maharashtra',
    "Manipur": 'manipur',
    "Meghalaya": 'meghalaya',
    "Mizoram": 'mizoram',
    "Nagaland": 'nagaland',
    "Odisha": 'odisha',
    "Puducherry": 'puducherry',
    "Punjab": 'punjab',
    "Rajasthan": 'rajasthan',
    "Sikkim": 'sikkim',
    "Tamil Nadu": 'tamil-nadu',
    "Telangana": 'telangana',
    "Uttar Pradesh": 'uttar-pradesh',
    "Uttarakhand": 'uttarakhand',
    "West Bengal": 'west-bengal'
}

TABLE_UDD = "mapped_user_devise_district"  # your table name

def to_float_cols(df, cols):
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
    return df

def to_int_options(series):
    return sorted({int(x) for x in pd.to_numeric(series, errors="coerce").dropna().tolist()})

st.title('PHONEPAY DATA ANALYSIS')
page = st.sidebar.radio("Navigation", ["Home", "Business Case Study"])
if page == "Home":
    st.header("Home Page")

elif page == "Business Case Study":
    st.header("Business Case Study")

    selected_option = st.selectbox("Select any Question", ["1. Decoding Transaction Dynamics on Phonepe",'2. Device Dominance & User Engagement','3. Insurance Penetration & Growth Potential', '4. Transaction Analysis for Market Expansion','5. User Engagement and Growth Strategy'])
    if selected_option == '1. Decoding Transaction Dynamics on Phonepe':
        st.markdown('<h1 style = "color: red;"> Total Transaction Analysis</h1>', unsafe_allow_html=True)
        sql_y = """ SELECT DISTINCT year, quater
                    FROM aggregated_transactions_stats
                    ORDER BY year, quater;"""
        mycursor.execute(sql_y)
        data = mycursor.fetchall()
        yq = pd.DataFrame(data, columns=['year',"quater"])
        col1, col2 = st.columns(2)
        with col1:
            yr = st.selectbox('Year', list(yq['year'].unique()))
        with col2:
            q=st.selectbox('quater',list(yq['quater'].unique()))
                                         
        m_data = map_data(int(yr), int(q))
        df = map_state(m_data)
        fig = px.choropleth(
            df,
            geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='state',
            color='Total_Transaction_Value',
            color_continuous_scale='blues'
        )
        fig.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig)

        st.markdown('<h1 style ="color: red;">Payment Method Popularity</h1>', unsafe_allow_html=True)
        sql = """SELECT
                    transaction_type,
                    SUM(transaction_count) as total_transaction_count,
                    SUM(transaction_amount) as total_transaction_amount
                FROM aggregated_transactions_stats
                GROUP BY Transaction_type
                ORDER BY total_transaction_count DESC;"""
        
        mycursor.execute(sql)
        data = mycursor.fetchall()
        df = pd.DataFrame(data, columns=["transaction_type","Total_Transaction_Count","Total_Transaction_Amount"])
        col1, col2 = st.columns(2)
        with col1:
            fig1 = px.pie(
                df,
                names="transaction_type",
                values="Total_Transaction_Count",
                title="Distribution of Total Transaction Count",
                hole = 0.4,
                labels={"transaction_type":"Transaction Type"},
            )
            st.plotly_chart(fig1)

        with col2:
            fig2 = px.pie(
                df,
                names="transaction_type",
                values="Total_Transaction_Amount",
                title="Distribution of Total Transaction AMount",
                hole = 0.4,
                labels={"transaction_type":"Transaction Type"}
            )
            st.plotly_chart(fig2)

        st.markdown('<h1 style="color:red;"> Top 10 State-wise Total Transaction Amount</h1>', unsafe_allow_html=True)
        sql2="""SELECT
                state,
                SUM(transaction_amount) as total_transaction_amount
            FROM aggregated_transactions_stats
            GROUP BY state
            ORDER BY total_transaction_amount DESC
            LIMIT 10;"""
        mycursor.execute(sql2)

        data = mycursor.fetchall()
        df2 = pd.DataFrame(data, columns=['state',"Total_Transaction_Amount"])
        fig3 = px.bar(
            df2,
            x="state",
            y="Total_Transaction_Amount",
            text="Total_Transaction_Amount",
            title="Total transaction Amount by State",
            labels={"state":"State","Total_Transaction_Amount":"Transaction Amount"},
        )
        fig3.update_traces(texttemplate='%{text:.2s}', textposition = 'outside')
        st.plotly_chart(fig3)
        st.markdown('<h1 style="color:red;"> Transaction by State and Payment Category</h1>',unsafe_allow_html=True)
        sql4="""SELECT
                state,
                transaction_type,
                SUM(transaction_count) as total_transactions,
                SUM(transaction_amount) as total_transaction_amount
            from aggregated_transactions_stats
            GROUP BY state, transaction_type
            ORDER BY state, total_transactions DESC;"""
        
        mycursor.execute(sql4)
        data = mycursor.fetchall()
        df3 = pd.DataFrame(data, columns=['state',"Transaction_type", "total_transaction","Total_Transaction_Amount"])
        state_options = df3["state"].unique()
        selected_state = st.selectbox("Select a State", state_options)
        filtered_df = df3[df3["state"]==selected_state]
        fig = px.line(
            filtered_df,
            x = "Transaction_type",
            y="Total_Transaction_Amount",
            title=f"Transaction Distribution in {selected_state.capitalize()}",
            labels={"Total_Transaction_Amount":"Transaction Amount", "Transaction_type":"Payment Category"},
            markers=True
        )
        st.plotly_chart(fig)
        st.markdown('<h1 style = "color: red;">Trend Analysis</h1>', unsafe_allow_html=True)
        sql6 = """ SELECT DISTINCT year, quater,      
                    SUM(transaction_count) AS total_transaction_count,
                    SUM(transaction_amount) AS total_transaction_amount
                    FROM aggregated_transactions_stats
                    GROUP BY year, quater
                    ORDER BY year, quater;"""
        mycursor.execute(sql6)
        data = mycursor.fetchall()
        df6 = pd.DataFrame(data, columns=['year',"quater","Total_Transaction_Count","Total_Transaction_Amount"])
        year_options = df6["year"].unique()
        selected_year = st.selectbox("Select a Year", year_options)
        filtered_df = df6[df6["year"]==selected_year]
        st.subheader(f"Transactions for Year {selected_year}")
        plt.figure(figsize=(10,6))
        plt.bar(filtered_df["quater"].astype(str), filtered_df["Total_Transaction_Amount"], color="skyblue")
        plt.xlabel("quater")
        plt.ylabel("Total Transaction Amount")
        plt.title(f"Transaction Amount Distribution for {selected_year}")
        plt.xticks(rotation = 0)
        st.pyplot(plt)
    
    elif selected_option == '2. Device Dominance & User Engagement':
        # --------- UI: Case Study 2 ----------
        st.markdown('<h1 style="color:red;">Device Dominance & User Engagement</h1>', unsafe_allow_html=True)

        # Filters
        meta = fetch_df(
            f"SELECT DISTINCT year, quarter FROM aggregated_user_state_totals ORDER BY year, quarter;",
            columns=["year","quarter"]
        )

        # Cast to plain Python ints (avoid numpy.int64 in params)
        year_opts = sorted({int(y) for y in meta["year"].tolist()})
        cs_year = int(st.selectbox("Year", year_opts))

        q_opts = sorted({int(q) for q in meta[meta["year"] == cs_year]["quarter"].tolist()})
        cs_quarter = int(st.selectbox("Quarter", q_opts))

        brand_opts = fetch_df(
            f"SELECT DISTINCT user_brand FROM aggregated_user_device_stats ORDER BY user_brand;",
            columns=["user_brand"]
        )["user_brand"].tolist()
        cs_brand = st.selectbox("Brand (for trend & diagnostics)", brand_opts) if brand_opts else None

        states_raw_df = fetch_df(
            f"SELECT DISTINCT state FROM aggregated_user_state_totals WHERE year=%s AND quarter=%s ORDER BY state;",
            (cs_year, cs_quarter),
            ["state"]
        )
        if states_raw_df.empty:
            states_pretty_opts = []
        else:
            states_pretty_opts = map_state_names(states_raw_df)["state"].dropna().unique().tolist()

        cs_state_pretty = st.selectbox("State (for brand mix)", states_pretty_opts) if states_pretty_opts else None
        cs_state_raw = INV_STATE.get(cs_state_pretty) if cs_state_pretty else None

        st.write("---")

        # 1) State choropleths (truth)
        st.subheader("State Overview (Ground Truth)")
        st.caption("Opens per User = app_opens / total_users")

        st_tot = fetch_df(
            f"""SELECT state, total_users, app_opens
                FROM aggregated_user_state_totals
                WHERE year=%s AND quarter=%s;""",
            (cs_year, cs_quarter),
            ["state","total_users","app_opens"]
        )
        st_tot_pretty = map_state_names(st_tot.copy()).dropna(subset=["state"])
        if not st_tot_pretty.empty:
            st_tot_pretty = to_float_cols(st_tot_pretty,["app_opens", "total_users"])
            st_tot_pretty["opens_per_user"] = (st_tot_pretty["app_opens"] / st_tot_pretty["total_users"]).round(2)
            tab1, tab2 = st.tabs(["Total Users", "Opens per User"])
            with tab1:
                fig_u = px.choropleth(
                    st_tot_pretty, geojson=GEOJSON_URL, featureidkey="properties.ST_NM",
                    locations="state", color="total_users", color_continuous_scale="blues",
                    title=f"Total Registered Users — Y{cs_year} Q{cs_quarter}"
                )
                fig_u.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig_u, use_container_width=True)
            with tab2:
                fig_o = px.choropleth(
                    st_tot_pretty, geojson=GEOJSON_URL, featureidkey="properties.ST_NM",
                    locations="state", color="opens_per_user", color_continuous_scale="blues",
                    title=f"Opens per User — Y{cs_year} Q{cs_quarter}"
                )
                fig_o.update_geos(fitbounds="locations", visible=False)
                st.plotly_chart(fig_o, use_container_width=True)
        else:
            st.info("No state totals for the selected period.")

        st.write("---")

        # 2) National Top 10 brands (users)
        st.subheader("Top 10 Brands by Registered Users (National)")
        top_b = fetch_df(
            f"""SELECT user_brand, SUM(user_count) AS users
                FROM aggregated_user_device_stats
                WHERE year=%s AND quarter=%s
                GROUP BY user_brand
                ORDER BY users DESC
                LIMIT 10;""",
            (cs_year, cs_quarter),
            ["user_brand","users"]
        )
        if not top_b.empty:
            fig1 = px.bar(top_b, x="user_brand", y="users", text="users",
                        title="Registered Users — Top 10 Brands",
                        labels={"user_brand":"Brand","users":"Users"})
            fig1.update_traces(textposition="outside", texttemplate="%{text:.3s}")
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("No brand totals for the selected period.")

        st.write("---")

        # 3) Brand mix in selected state (users + estimated opens)
        st.subheader("Brand Mix Inside Selected State")
        st.caption("Estimated opens by brand = state app_opens x user_percentage (proportional).")

        if cs_state_raw:
            mix = fetch_df(
                f"""SELECT d.user_brand,
                        SUM(d.user_count) AS users,
                        ROUND(SUM(d.user_percentage)*100, 2) AS pct,
                        MAX(t.app_opens) AS state_opens
                    FROM aggregated_user_device_stats d
                    JOIN aggregated_user_state_totals t
                    ON t.state=d.state AND t.year=d.year AND t.quarter=d.quarter
                    WHERE d.year=%s AND d.quarter=%s AND d.state=%s
                    GROUP BY d.user_brand;""",
                (cs_year, cs_quarter, cs_state_raw),
                ["user_brand","users","pct","state_opens"]
            )
            if not mix.empty:
                mix = to_float_cols(mix, ["pct","state_opens","users"])
                mix["est_opens"] = (mix["pct"]/100.0 * mix["state_opens"]).round()
                c1, c2 = st.columns(2)
                with c1:
                    figp = px.pie(mix, names="user_brand", values="users",
                                title=f"Registered Users by Brand — {cs_state_pretty}")
                    st.plotly_chart(figp, use_container_width=True)
                with c2:
                    figb = px.bar(mix.sort_values("est_opens", ascending=False),
                                x="user_brand", y="est_opens", text="est_opens",
                                title=f"(Estimated) App Opens by Brand — {cs_state_pretty}",
                                labels={"user_brand":"Brand","est_opens":"Estimated Opens"})
                    figb.update_traces(textposition="outside", texttemplate="%{text:.3s}")
                    st.plotly_chart(figb, use_container_width=True)
            else:
                st.info("No brand mix for the selected state/period.")
        else:
            st.info("Select a state to see its brand mix.")

        st.write("---")

        # 4) Under-utilization detector (estimated)
        st.subheader("Under-Utilization Detector (Estimated)")
        st.caption("Flags brand-state pairs where estimated opens/user < 80% of that brand's national estimated mean.")

        nat = fetch_df(
            f"""SELECT d.user_brand,
                    SUM(d.user_count) AS users,
                    SUM(t.app_opens * d.user_percentage) AS est_opens
                FROM aggregated_user_device_stats d
                JOIN aggregated_user_state_totals t
                ON t.state=d.state AND t.year=d.year AND t.quarter=d.quarter
                WHERE d.year=%s AND d.quarter=%s
                GROUP BY d.user_brand
                HAVING SUM(d.user_count) > 0;""",
            (cs_year, cs_quarter),
            ["user_brand","users","est_opens"]
        )
        if not nat.empty:
            nat = to_float_cols(nat, ["users", "est_opens"])
            nat["brand_est_opu"] = (nat["est_opens"] / nat["users"]).round(4)
            sb = fetch_df(
                f"""SELECT d.state, d.user_brand,
                        SUM(d.user_count) AS users,
                        SUM(t.app_opens * d.user_percentage) AS est_opens
                    FROM aggregated_user_device_stats d
                    JOIN aggregated_user_state_totals t
                    ON t.state=d.state AND t.year=d.year AND t.quarter=d.quarter
                    WHERE d.year=%s AND d.quarter=%s
                    GROUP BY d.state, d.user_brand
                    HAVING SUM(d.user_count) > 0;""",
                (cs_year, cs_quarter),
                ["state","user_brand","users","est_opens"]
            )
            if not sb.empty:
                sb = to_float_cols(sb, ["users", "est_opens"])
                sb["est_opu"] = (sb["est_opens"] / sb["users"]).round(4)
                sb = sb.merge(nat[["user_brand","brand_est_opu"]], on="user_brand", how="left")
                sb["util_ratio"] = (sb["est_opu"] / sb["brand_est_opu"]).round(3)
                under = sb[sb["util_ratio"] < 0.8].copy().sort_values("util_ratio")
                under_pretty = map_state_names(under, col="state").dropna(subset=["state"])
                st.dataframe(
                    under_pretty[["state","user_brand","users","est_opu","brand_est_opu","util_ratio"]]
                    .rename(columns={
                        "users":"Users",
                        "est_opu":"Est Opens/User",
                        "brand_est_opu":"Brand Nat. Est OPU",
                        "util_ratio":"Utilization / Brand Mean"
                    })
                )
            else:
                st.info("No brand–state records to evaluate.")
        else:
            st.info("No national brand baseline to compare.")

        st.write("---")

        # 5) Quarterly trend (users + estimated opens) for selected brand
        if cs_brand:
            st.subheader(f"Quarterly Trend — {cs_brand} (Users & Estimated Opens) in {cs_year}")
            trend = fetch_df(
                f"""SELECT d.quarter,
                        SUM(d.user_count) AS users,
                        SUM(t.app_opens * d.user_percentage) AS est_opens
                    FROM aggregated_user_device_stats d
                    JOIN aggregated_user_state_totals t
                    ON t.state=d.state AND t.year=d.year AND t.quarter=d.quarter
                    WHERE d.year=%s AND d.user_brand=%s
                    GROUP BY d.quarter
                    ORDER BY d.quarter;""",
                (cs_year, cs_brand),
                ["quarter","users","est_opens"]
            )
            if not trend.empty:
                trend = to_float_cols(trend, ["users", "est_opens"])
                trend["quarter"] = trend["quarter"].astype(int)
                figt = px.line(trend, x="quarter", y=["users","est_opens"], markers=True,
                            title=f"{cs_brand}: Users & (Estimated) Opens by Quarter — {cs_year}",
                            labels={"value":"Count","variable":"Metric"})
                st.plotly_chart(figt, use_container_width=True)
            else:
                st.info("No quarterly trajectory for the selected brand/year.")

        st.write("---")

        # 6) Heatmap: State × Brand (users)
        st.subheader("Heatmap: Registered Users by State × Brand (Selected Y/Q)")
        hm = fetch_df(
            f"""SELECT state, user_brand, SUM(user_count) AS users
                FROM aggregated_user_device_stats
                WHERE year=%s AND quarter=%s
                GROUP BY state, user_brand;""",
            (cs_year, cs_quarter),
            ["state","user_brand","users"]
        )
        hm_pretty = map_state_names(hm.copy()).dropna(subset=["state"])
        if not hm_pretty.empty:
            pivot_df = hm_pretty.pivot_table(index="state", columns="user_brand", values="users", aggfunc="sum", fill_value=0)
            fig_hm = px.imshow(pivot_df, labels=dict(x="Brand", y="State", color="Users"),
                            title="Registered Users by State × Brand")
            st.plotly_chart(fig_hm, use_container_width=True)
        else:
            st.info("No state × brand matrix for the selected period.")

    elif selected_option == '3. Insurance Penetration & Growth Potential':
        st.markdown('<h1 style="color:red;">Insurance Penetration & Growth Potential</h1>', unsafe_allow_html=True)
        
        # --- Case Study 3 filters (Year + Quarter) ---
        meta = fetch_df(
            "SELECT DISTINCT year, quater FROM aggregated_insurance_stats ORDER BY year, quater;",
            columns=["year","quater"]
        )

        year_opts = to_int_options(meta["year"]) if not meta.empty else []
        if not year_opts:
            st.warning("No insurance data available yet.")
            st.stop()

        cs_year = st.selectbox("Year", year_opts)  

        # Build safe quarter options for chosen year
        meta_year_numeric = pd.to_numeric(meta["year"], errors="coerce")
        q_opts = to_int_options(meta.loc[meta_year_numeric == cs_year, "quater"])

        if not q_opts:
            st.warning(f"No quarters found for Year {cs_year}.")
            st.stop()

        cs_quarter = st.selectbox("Quarter", q_opts)  

        # 1) Choropleth: Insurance Count by State
        df1 = fetch_df(
            """SELECT state, insurance_count, insurance_amount
            FROM aggregated_insurance_stats
            WHERE year=%s AND quater=%s;""",
            (cs_year, cs_quarter), ["state","insurance_count","insurance_amount"]
        )
        df1_pretty = map_state_names(df1.copy()).dropna(subset=["state"])

        fig1 = px.choropleth(
            df1_pretty, geojson=GEOJSON_URL, featureidkey="properties.ST_NM",
            locations="state", color="insurance_count", color_continuous_scale="Blues",
            title=f"Insurance Transactions Count — Y{cs_year} Q{cs_quarter}"
        )
        fig1.update_geos(fitbounds="locations", visible=False)
        st.plotly_chart(fig1, use_container_width=True)

        # 2) Top 10 States by Insurance Amount
        top_states = df1_pretty.nlargest(10, "insurance_amount")
        fig2 = px.bar(top_states, x="state", y="insurance_amount", text="insurance_amount",
                    title="Top 10 States by Insurance Amount")
        fig2.update_traces(textposition="outside", texttemplate="%{text:.3s}")
        st.plotly_chart(fig2, use_container_width=True)

        # 3) Trend Analysis across quarters
        trend = fetch_df(
            """SELECT year, quater,
                    SUM(insurance_count) AS total_count,
                    SUM(insurance_amount) AS total_amount
            FROM aggregated_insurance_stats
            GROUP BY year, quater
            ORDER BY year, quater;""",
            columns=["year","quater","total_count","total_amount"]
        )
        trend["period"] = trend["year"].astype(str) + "-Q" + trend["quater"].astype(str)

        fig3 = px.line(trend, x="period", y="total_amount", markers=True,
                    title="Insurance Amount Trend Over Time")
        st.plotly_chart(fig3, use_container_width=True)


    elif selected_option == '4. Transaction Analysis for Market Expansion':
        st.markdown('<h1 style="color:red;">Transaction Analysis for Market Expansion</h1>', unsafe_allow_html=True)

        # Level selector
        level = st.radio("Analysis Level", ["State", "District"], horizontal=True)

        # Year/Quarter filters (safe options)
        meta = fetch_df(
            "SELECT DISTINCT year, quarter FROM mapped_transaction_district_totals ORDER BY year, quarter;",
            columns=["year","quarter"]
        )
        year_opts = sorted({int(x) for x in pd.to_numeric(meta["year"], errors="coerce").dropna().tolist()})
        cs_year = st.selectbox("Year", year_opts) if year_opts else st.stop()
        q_opts = sorted({int(x) for x in pd.to_numeric(meta.loc[pd.to_numeric(meta["year"], errors='coerce')==cs_year,"quarter"], errors="coerce").dropna().tolist()})
        cs_quarter = st.selectbox("Quarter", q_opts) if q_opts else st.stop()

        # If District view, choose a state first (from state totals table for that Y/Q)
        cs_state_raw, cs_state_pretty = None, None
        if level == "District":
            states_df = fetch_df(
                """SELECT DISTINCT state FROM mapped_transaction_district_totals
                WHERE year=%s AND quarter=%s ORDER BY state;""",
                (cs_year, cs_quarter), ["state"]
            )
            states_pretty = map_state_names(states_df)["state"].dropna().unique().tolist() if not states_df.empty else []
            cs_state_pretty = st.selectbox("State (for district analysis)", states_pretty) if states_pretty else None
            # use your INV_STATE dict from earlier
            cs_state_raw = INV_STATE.get(cs_state_pretty) if cs_state_pretty else None

        # === 3.2 STATE-LEVEL VIEWS ===

        # 1) Choropleth: state-wise transaction AMOUNT (selected Y/Q)
        st.subheader("State-wise Transaction Amount (Selected Year–Quarter)")
        df_state = fetch_df(
            """
            SELECT state, transaction_count, transaction_amount
            FROM mapped_transaction_district_totals
            WHERE year=%s AND quarter=%s;
            """,
            (cs_year, cs_quarter),
            ["state", "transaction_count", "transaction_amount"]
        )

        if df_state.empty:
            st.info("No state data for the selected period.")
        else:
            df_state = to_float_cols(df_state, ["transaction_count", "transaction_amount"])
            df_state_pretty = map_state_names(df_state.copy()).dropna(subset=["state"])

            fig_choro = px.choropleth(
                df_state_pretty,
                geojson=GEOJSON_URL,
                featureidkey="properties.ST_NM",
                locations="state",
                color="transaction_amount",
                color_continuous_scale="OrRd",
                title=f"Transaction Amount by State — Y{cs_year} Q{cs_quarter}"
            )
            fig_choro.update_geos(fitbounds="locations", visible=False)
            st.plotly_chart(fig_choro, use_container_width=True)

        # 2) Top-10 states — by COUNT and by AMOUNT
        if not df_state.empty:
            c1, c2 = st.columns(2)

            with c1:
                top_count = df_state_pretty.nlargest(10, "transaction_count")
                fig_top_count = px.bar(
                    top_count,
                    x="state", y="transaction_count", text="transaction_count",
                    title="Top 10 States by Transaction Count",
                    labels={"state": "State", "transaction_count": "Count"}
                )
                fig_top_count.update_traces(textposition="outside", texttemplate="%{text:.3s}")
                st.plotly_chart(fig_top_count, use_container_width=True)

            with c2:
                top_amount = df_state_pretty.nlargest(10, "transaction_amount")
                fig_top_amount = px.bar(
                    top_amount,
                    x="state", y="transaction_amount", text="transaction_amount",
                    title="Top 10 States by Transaction Amount",
                    labels={"state": "State", "transaction_amount": "Amount"}
                )
                fig_top_amount.update_traces(textposition="outside", texttemplate="%{text:.3s}")
                st.plotly_chart(fig_top_amount, use_container_width=True)

        # 3) National trend over time
        st.subheader("National Transaction Trend (All States Aggregated)")
        trend = fetch_df(
            """
            SELECT year, quarter,
                SUM(transaction_count)  AS total_count,
                SUM(transaction_amount) AS total_amount
            FROM mapped_transaction_district_totals
            GROUP BY year, quarter
            ORDER BY year, quarter;
            """,
            columns=["year", "quarter", "total_count", "total_amount"]
        )

        if trend.empty:
            st.info("No national trend data available.")
        else:
            trend = to_float_cols(trend, ["total_count", "total_amount"])
            # Build a tidy label like 2021-Q1, ensuring ints
            trend["year"] = pd.to_numeric(trend["year"], errors="coerce").astype("Int64")
            trend["quarter"] = pd.to_numeric(trend["quarter"], errors="coerce").astype("Int64")
            trend["period"] = trend["year"].astype(str) + "-Q" + trend["quarter"].astype(str)

            tab_cnt, tab_amt = st.tabs(["Total Count", "Total Amount"])
            with tab_cnt:
                fig_trend_cnt = px.line(
                    trend, x="period", y="total_count", markers=True,
                    title="National Transaction Count Over Time",
                    labels={"period": "Period", "total_count": "Total Count"}
                )
                st.plotly_chart(fig_trend_cnt, use_container_width=True)

            with tab_amt:
                fig_trend_amt = px.line(
                    trend, x="period", y="total_amount", markers=True,
                    title="National Transaction Amount Over Time",
                    labels={"period": "Period", "total_amount": "Total Amount"}
                )
                st.plotly_chart(fig_trend_amt, use_container_width=True)


        if level == "District":
            if not cs_state_raw:
                st.info("Select a state to view its districts.")
            else:
                # 1) District table for selected state
                dist = fetch_df(
                    """SELECT district, transaction_count, transaction_amount
                    FROM mapped_transaction_district_totals
                    WHERE state=%s AND year=%s AND quarter=%s
                    ORDER BY transaction_amount DESC;""",
                    (cs_state_raw, cs_year, cs_quarter),
                    ["district","transaction_count","transaction_amount"]
                )

                if dist.empty:
                    st.warning(f"No district data found for {cs_state_pretty} (Y{cs_year} Q{cs_quarter}).")
                else:
                    # Clean & show top 15 by amount
                    dist["district"] = dist["district"].str.title()
                    dist = to_float_cols(dist, ["transaction_count","transaction_amount"])

                    st.subheader(f"Top Districts in {cs_state_pretty} — Y{cs_year} Q{cs_quarter}")
                    topN = st.slider("Show top N districts (by amount)", 5, 30, 15)
                    top_d = dist.nlargest(topN, "transaction_amount")

                    c1, c2 = st.columns(2)
                    with c1:
                        fig_b_amt = px.bar(
                            top_d, x="district", y="transaction_amount", text="transaction_amount",
                            title="Top Districts by Transaction Amount",
                            labels={"district":"District","transaction_amount":"Amount"}
                        )
                        fig_b_amt.update_traces(textposition="outside", texttemplate="%{text:.3s}")
                        st.plotly_chart(fig_b_amt, use_container_width=True)

                    with c2:
                        top_c = dist.nlargest(topN, "transaction_count")
                        fig_b_cnt = px.bar(
                            top_c, x="district", y="transaction_count", text="transaction_count",
                            title="Top Districts by Transaction Count",
                            labels={"district":"District","transaction_count":"Count"}
                        )
                        fig_b_cnt.update_traces(textposition="outside", texttemplate="%{text:.3s}")
                        st.plotly_chart(fig_b_cnt, use_container_width=True)

                    st.write("---")

                    # 2) Efficiency scatter: value vs count (helps spot high-value/low-volume and vice versa)
                    st.subheader("District Efficiency — Value vs Volume")
                    fig_sc = px.scatter(    
                        dist,
                        x="transaction_count",
                        y="transaction_amount",
                        hover_name="district",
                        labels={
                            "transaction_count": "Transactions (Count)",
                            "transaction_amount": "Transaction Value (Amount)"
                        },
                        title=f"Value vs Volume — {cs_state_pretty} (Y{cs_year} Q{cs_quarter})"
                    )
                    st.plotly_chart(fig_sc, use_container_width=True)

                    st.write("---")

                    # 3 bottom districts (opportunity pockets)
                    st.subheader("Bottom Districts (Opportunity Pockets)")
                    maxN = min(30, len(dist))
                    if maxN < 5:
                        bottomN = maxN  # no slider needed, just use all
                    else:
                        bottomN = st.slider(
                            "Show bottom N districts (by amount)",
                            min_value=5,
                            max_value=maxN,
                            value=min(10, maxN)
                    )
                    bot = dist.nsmallest(bottomN, "transaction_amount")
                    fig_bot = px.bar(
                        bot, x="district", y="transaction_amount", text="transaction_amount",
                        title="Bottom Districts by Amount",
                        labels={"district":"District","transaction_amount":"Amount"}
                    )
                    fig_bot.update_traces(textposition="outside", texttemplate="%{text:.3s}")
                    st.plotly_chart(fig_bot, use_container_width=True)

                    st.write("---")

                    # 4) District share of state total (helps quantify concentration)
                    state_total = dist["transaction_amount"].sum()
                    dist["state_share_pct"] = (dist["transaction_amount"] / state_total * 100.0).round(2)
                    fig_share = px.bar(
                        dist.sort_values("state_share_pct", ascending=False).head(topN),
                        x="district", y="state_share_pct", text="state_share_pct",
                        title="District Share of State Amount (Top N)",
                        labels={"district":"District","state_share_pct":"Share (%)"}
                    )
                    fig_share.update_traces(textposition="outside")
                    st.plotly_chart(fig_share, use_container_width=True)

    if selected_option == '5. User Engagement and Growth Strategy':
        st.markdown('<h1 style="color:red;">User Engagement & Growth Strategy</h1>', unsafe_allow_html=True)

        # Level: State or District
        level = st.radio("Analysis Level", ["State", "District"], horizontal=True)

        # Year/Quarter filters (safe)
        meta = fetch_df(
            f"SELECT DISTINCT year, quarter FROM {TABLE_UDD} ORDER BY year, quarter;",
            columns=["year","quarter"]
        )
        year_opts = to_int_options(meta["year"]) if not meta.empty else []
        if not year_opts:
            st.warning("No data available yet."); st.stop()
        cs_year = st.selectbox("Year", year_opts)

        meta_year_num = pd.to_numeric(meta["year"], errors="coerce")
        q_opts = to_int_options(meta.loc[meta_year_num == cs_year, "quarter"])
        if not q_opts:
            st.warning(f"No quarters found for Year {cs_year}."); st.stop()
        cs_quarter = st.selectbox("Quarter", q_opts)

        # If District view, select state
        cs_state_raw, cs_state_pretty = None, None
        if level == "District":
            states_df = fetch_df(
                f"""SELECT DISTINCT state FROM {TABLE_UDD}
                    WHERE year=%s AND quarter=%s ORDER BY state;""",
                (cs_year, cs_quarter), ["state"]
            )
            states_pretty = map_state_names(states_df)["state"].dropna().unique().tolist() if not states_df.empty else []
            cs_state_pretty = st.selectbox("State (for district analysis)", states_pretty) if states_pretty else None
            cs_state_raw = INV_STATE.get(cs_state_pretty) if cs_state_pretty else None

        st.write("---")

        # =========================
        # KPI cards (selected Y/Q – State totals)
        # =========================
        kpi = fetch_df(
            f"""SELECT state,
                    SUM(registered_users) AS users,
                    SUM(app_opens)        AS opens
                FROM {TABLE_UDD}
                WHERE year=%s AND quarter=%s
                GROUP BY state;""",
            (cs_year, cs_quarter), ["state","users","opens"]
        )
        if not kpi.empty:
            kpi = to_float_cols(kpi, ["users","opens"])
            total_users = int(kpi["users"].sum())
            total_opens = int(kpi["opens"].sum())
            opu = round(total_opens / total_users, 2) if total_users else 0

            c1, c2, c3 = st.columns(3)
            c1.metric("Total Registered Users", f"{total_users:,}")
            c2.metric("Total App Opens", f"{total_opens:,}")
            c3.metric("Opens per User (National)", f"{opu:.2f}")
        else:
            st.info("No KPI data for the selected period.")

        st.write("---")

        # =========================
        # State-level views
        # =========================
        if level == "State":
            # Aggregate district→state for the selected Y/Q
            st_df = fetch_df(
                f"""SELECT state,
                        SUM(registered_users) AS users,
                        SUM(app_opens)        AS opens
                    FROM {TABLE_UDD}
                    WHERE year=%s AND quarter=%s
                    GROUP BY state;""",
                (cs_year, cs_quarter), ["state","users","opens"]
            )

            if st_df.empty:
                st.info("No state data for the selected period.")
            else:
                st_df = to_float_cols(st_df, ["users","opens"])
                st_df["opens_per_user"] = (st_df["opens"] / st_df["users"]).replace([np.inf, -np.inf], np.nan).round(2)
                st_df_pretty = map_state_names(st_df.copy()).dropna(subset=["state"])

                tabs = st.tabs(["Opens per User (Map)", "Top States", "Efficiency Scatter", "National Trend"])
                with tabs[0]:
                    fig = px.choropleth(
                        st_df_pretty, geojson=GEOJSON_URL, featureidkey="properties.ST_NM",
                        locations="state", color="opens_per_user", color_continuous_scale="Blues",
                        title=f"Opens per User — Y{cs_year} Q{cs_quarter}"
                    )
                    fig.update_geos(fitbounds="locations", visible=False)
                    st.plotly_chart(fig, use_container_width=True)

                with tabs[1]:
                    cA, cB = st.columns(2)
                    with cA:
                        top_opu = st_df_pretty.nlargest(10, "opens_per_user")
                        fig_top_opu = px.bar(
                            top_opu, x="state", y="opens_per_user", text="opens_per_user",
                            title="Top 10 States by Opens/User",
                            labels={"state":"State","opens_per_user":"Opens/User"}
                        )
                        fig_top_opu.update_traces(textposition="outside")
                        st.plotly_chart(fig_top_opu, use_container_width=True)
                    with cB:
                        top_users = st_df_pretty.nlargest(10, "users")
                        fig_top_users = px.bar(
                            top_users, x="state", y="users", text="users",
                            title="Top 10 States by Registered Users",
                            labels={"state":"State","users":"Users"}
                        )
                        fig_top_users.update_traces(textposition="outside", texttemplate="%{text:.3s}")
                        st.plotly_chart(fig_top_users, use_container_width=True)

                with tabs[2]:
                    # Efficiency: volume vs engagement
                    fig_sc = px.scatter(
                        st_df_pretty, x="users", y="opens",
                        hover_name="state",
                        labels={"users":"Registered Users","opens":"App Opens"},
                        title=f"Value vs Volume — State Efficiency (Y{cs_year} Q{cs_quarter})"
                    )
                    st.plotly_chart(fig_sc, use_container_width=True)

                with tabs[3]:
                    trend = fetch_df(
                        f"""SELECT year, quarter,
                                SUM(registered_users) AS users,
                                SUM(app_opens)        AS opens
                            FROM {TABLE_UDD}
                            GROUP BY year, quarter
                            ORDER BY year, quarter;""",
                        columns=["year","quarter","users","opens"]
                    )
                    if not trend.empty:
                        trend = to_float_cols(trend, ["users","opens"])
                        trend["period"] = pd.to_numeric(trend["year"], errors="coerce").astype("Int64").astype(str) + \
                                        "-Q" + pd.to_numeric(trend["quarter"], errors="coerce").astype("Int64").astype(str)
                        t1, t2 = st.columns(2)
                        with t1:
                            fig_cnt = px.line(trend, x="period", y="users", markers=True,
                                            title="National Registered Users Trend")
                            st.plotly_chart(fig_cnt, use_container_width=True)
                        with t2:
                            fig_opn = px.line(trend, x="period", y="opens", markers=True,
                                            title="National App Opens Trend")
                            st.plotly_chart(fig_opn, use_container_width=True)
                    else:
                        st.info("No national trend data yet.")

        # =========================
        # District-level views
        # =========================
        if level == "District":
            if not cs_state_raw:
                st.info("Select a state to view its districts.")
            else:
                dist = fetch_df(
                    f"""SELECT district, SUM(registered_users) AS users, SUM(app_opens) AS opens
                        FROM {TABLE_UDD}
                        WHERE state=%s AND year=%s AND quarter=%s
                        GROUP BY district
                        ORDER BY opens DESC;""",
                    (cs_state_raw, cs_year, cs_quarter),
                    ["district","users","opens"]
                )
                if dist.empty:
                    st.warning(f"No district data for {cs_state_pretty} (Y{cs_year} Q{cs_quarter}).")
                else:
                    dist["district"] = dist["district"].astype(str).str.strip().str.title()
                    dist = to_float_cols(dist, ["users","opens"])
                    dist["opens_per_user"] = (dist["opens"] / dist["users"]).replace([np.inf, -np.inf], np.nan).round(3)

                    st.subheader(f"District Engagement — {cs_state_pretty} (Y{cs_year} Q{cs_quarter})")

                    # Top N by opens_per_user
                    maxN = min(30, len(dist))
                    if maxN < 5:
                        topN = maxN
                    else:
                        topN = st.slider("Show top N districts (by Opens/User)", 5, maxN, min(10, maxN))
                    top_opu = dist.nlargest(topN, "opens_per_user")

                    c1, c2 = st.columns(2)
                    with c1:
                        fig_top_opu = px.bar(
                            top_opu, x="district", y="opens_per_user", text="opens_per_user",
                            title="Top Districts by Opens/User",
                            labels={"district":"District","opens_per_user":"Opens/User"}
                        )
                        fig_top_opu.update_traces(textposition="outside")
                        st.plotly_chart(fig_top_opu, use_container_width=True)

                    with c2:
                        top_users = dist.nlargest(min(10, len(dist)), "users")
                        fig_top_users = px.bar(
                            top_users, x="district", y="users", text="users",
                            title="Top Districts by Registered Users",
                            labels={"district":"District","users":"Users"}
                        )
                        fig_top_users.update_traces(textposition="outside", texttemplate="%{text:.3s}")
                        st.plotly_chart(fig_top_users, use_container_width=True)

                    st.write("---")

                    # Efficiency scatter (no statsmodels dependency)
                    fig_sc = px.scatter(
                        dist, x="users", y="opens",
                        hover_name="district",
                        labels={"users":"Registered Users","opens":"App Opens"},
                        title=f"Value vs Volume — {cs_state_pretty} (Districts)"
                    )
                    st.plotly_chart(fig_sc, use_container_width=True)

                    st.write("---")

                    # Bottom N (opportunity pockets) by opens_per_user
                    if maxN < 5:
                        bottomN = maxN
                    else:
                        bottomN = st.slider("Show bottom N districts (by Opens/User)", 5, maxN, min(10, maxN))
                    bottom = dist.nsmallest(bottomN, "opens_per_user")
                    fig_bot = px.bar(
                        bottom, x="district", y="opens_per_user", text="opens_per_user",
                        title="Bottom Districts by Opens/User (Opportunity)",
                        labels={"district":"District","opens_per_user":"Opens/User"}
                    )
                    fig_bot.update_traces(textposition="outside")
                    st.plotly_chart(fig_bot, use_container_width=True)