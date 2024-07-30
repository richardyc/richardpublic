import io
import streamlit as st
import pandas as pd
import requests
from PIL import Image
from io import BytesIO
import base64
import os
import json
from openai import AsyncOpenAI
import instructor
import asyncio
import aiohttp
from xlsxwriter.utility import xl_rowcol_to_cell

from pydantic import BaseModel, Field
from typing import Optional, List

# Load secrets from Streamlit
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise EnvironmentError("OPENAI_API_KEY environment variable not found")

google_maps_api_key = os.getenv("GOOGLE_MAPS_API_KEY")
if not google_maps_api_key:
    raise EnvironmentError("GOOGLE_MAPS_API_KEY environment variable not found")

helicone_ai_api_key = os.getenv("HELICONE_API_KEY")
if not helicone_ai_api_key:
    raise EnvironmentError("HELICONE_AI_API_KEY environment variable not found")

# Add password protection
if not os.getenv("APP_PASSWORD"):
    st.warning("No password set. Set the environment variable `APP_PASSWORD` to enable password protection.")

# Initialize OpenAI client
client = instructor.from_openai(AsyncOpenAI(
    base_url="https://oai.helicone.ai/v1",
    api_key=api_key,
    default_headers= {  # Optionally set default headers or set per request (see below)
        "Helicone-Auth": f"Bearer {helicone_ai_api_key}",
        "Helicone-Cache-Enabled": "true",
    }
))

# Configurable batch size
ASYNC_BATCH_SIZE = 15

def extract_coordinates(google_maps_link):
    # Extract latitude and longitude from Google Maps link
    parts = google_maps_link.split('!')
    lat = lon = None
    for part in parts:
        if part.startswith('3d'):
            lat = float(part[2:])
        elif part.startswith('4d'):
            lon = float(part[2:])
    return lat, lon

async def get_google_satellite_image(session, lat, lon, api_key, zoom):
    endpoint = "https://maps.googleapis.com/maps/api/staticmap"
    params = {
        'center': f'{lat},{lon}',
        'zoom': zoom,
        'size': '1024x1024',
        'maptype': 'satellite',
        'key': api_key
    }
    async with session.get(endpoint, params=params) as response:
        if response.status == 200:
            image_data = await response.read()
            image = Image.open(BytesIO(image_data))
            return image
        else:
            raise Exception(f"Error fetching image: {response.status} - {await response.text()}")

def image_to_base64(image):
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode()

class GoogleSatelliteAnalysis(BaseModel):
    calculations: str = Field(description="Explain your calculations for each field from the images provided.")
    is_warehouse: bool = Field(description="From the satellite images, does this location at the center of the image look like a warehouse (with docks). Explanation: Look for rectangular buildings with loading docks, truck parking areas, and large open spaces around the structure.")
    total_dock_count: Optional[int] = Field(default=None, description="Based on the images approx how many docks are there for this warehouse. Explanation: Count visible loading dock doors or estimate based on the building's perimeter and typical dock spacing.")
    trucks_on_dock: Optional[int] = Field(default=None, description="From the images how many trucks are on the dock. Explanation: Count visible trucks parked at docks. Look for rectangular shapes that are typical of semi-trailers.")
    warehouse_size: Optional[int] = Field(default=None, description="The approx size of the warehouse in sqft (best approximate). Explanation: Estimate the length and width of the building, then multiply to get the square footage. Use known objects like trucks for scale.", example="20000")
    employee_count: Optional[int] = Field(default=None, description="Based on warehouse_size, best estimate. Explanation: Use industry standards (e.g., 1 employee per 1000-1500 sqft for typical warehouses) and adjust based on visible parking lot size and occupancy.")

async def analyze_image(image_base64_zoom19, image_base64_zoom18):
    response = await client.chat.completions.create(
        model="gpt-4o-mini",
        response_model=GoogleSatelliteAnalysis,
        messages=[
            {
                "role": "system",
                "content": "You are an urban planner and expert statistician."
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Analyze only the building at the center of these Google Maps satellite images. One image contains a google satellite image (~450ft*450ft) and another zoomed out google satellite image (~900ft*900ft). Is this location a warehouse? If so, provide details about its size, estimated employee count, number of trucks on docks, and total dock count. If it's not a warehouse or you're unsure, indicate that in your response. Be strict and accurate with your estimation."},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{image_base64_zoom19}"
                        }
                    },
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{image_base64_zoom18}"
                        }
                    }
                ]
            }
        ],
        max_tokens=1500,
        extra_headers={
            "Helicone-User-id": "Streamlit Testing"
        }
    )
    # print(response)

    return response

async def process_batch(batch, session, maps_link_column):
    tasks = []
    for row in batch:
        lat, lon = extract_coordinates(row[maps_link_column])
        if lat and lon:
            task = asyncio.create_task(process_row(session, row, lat, lon))
            tasks.append(task)
    results = await asyncio.gather(*tasks)
    return results

async def process_row(session, row, lat, lon):
    try:
        image_zoom19 = await get_google_satellite_image(session, lat, lon, google_maps_api_key, 19)
        image_zoom18 = await get_google_satellite_image(session, lat, lon, google_maps_api_key, 18)
        image_base64_zoom19 = image_to_base64(image_zoom19)
        image_base64_zoom18 = image_to_base64(image_zoom18)
        analysis = await analyze_image(image_base64_zoom19, image_base64_zoom18)

        if analysis:
            return {
                **row,
                'latitude': lat,
                'longitude': lon,
                'image': image_base64_zoom19,
                'is_warehouse': analysis.is_warehouse,
                'warehouse_size_sqft': analysis.warehouse_size,
                'employee_count': analysis.employee_count,
                'trucks_on_dock': analysis.trucks_on_dock,
                'total_dock_count': analysis.total_dock_count,
                'calculations': analysis.calculations
            }
        else:
            st.warning(f"Analysis failed for row: {row}")
            return None
    except Exception as e:
        st.error(f"Error processing row: {str(e)}")
        return None

async def process_csv(df, maps_link_column, preview=False):
    results = []
    total_rows = len(df) if not preview else min(10, len(df))
    progress_bar = st.progress(0)
    progress_bar.max = total_rows
    current_progress = 0
    progress_text = st.empty()
    
    async with aiohttp.ClientSession() as session:
        for i in range(0, total_rows, ASYNC_BATCH_SIZE):
            batch = df.iloc[i:min(i+ASYNC_BATCH_SIZE, total_rows)].to_dict('records')
            batch_results = await process_batch(batch, session, maps_link_column)
            current_progress += len(batch_results)
            progress_bar.progress(current_progress)
            results.extend([r for r in batch_results if r is not None])
            progress_text.text(f"{len(results)}/{total_rows} rows completed")
    
    progress_bar.empty()
    progress_text.empty()
    return pd.DataFrame(results)

def display_preview(df):
    # Display a preview of the results
    display_df = df.copy()
    
    # Convert base64 images to HTML img tags
    def base64_to_img_tag(base64_string):
        return f'<img src="data:image/png;base64,{base64_string}" style="width:100px;height:auto;">'
    
    display_df['image'] = display_df['image'].apply(base64_to_img_tag)
    
    # Convert specific columns to int type, handling NaN values
    int_columns = ['warehouse_size_sqft', 'employee_count', 'trucks_on_dock', 'total_dock_count']
    for col in int_columns:
        if col in display_df.columns:
            display_df[col] = display_df[col].fillna(0).astype(int)
    
    # Convert DataFrame to HTML
    html = display_df.to_html(escape=False, index=False)
    
    # Display the table with images
    st.write(html, unsafe_allow_html=True)

def display_results(result_df):
    # Create an Excel file in memory
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Remove 'image' column and add 'image_preview' column to the DataFrame before writing to Excel
        result_df_for_excel = result_df.drop(columns=['image'])
        result_df_for_excel['image_preview'] = ''  # Placeholder for image preview
        result_df_for_excel.to_excel(writer, sheet_name='Results', index=False)
        worksheet = writer.sheets['Results']
        
        # Adjust column widths
        for idx, col in enumerate(result_df_for_excel.columns):
            series = result_df_for_excel[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
            )) + 1  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)
        
        # Insert images into the 'image_preview' column
        if 'image' in result_df.columns:
            image_preview_column = result_df_for_excel.columns.get_loc('image_preview')
            for row_num, image_base64 in enumerate(result_df['image'], start=2):  # start=2 to account for header row
                if image_base64:
                    # Decode the base64 image
                    image_data = io.BytesIO(base64.b64decode(image_base64))
                    # Open the image using Pillow
                    with Image.open(image_data) as img:
                        # Resize the image to 200x200
                        img = img.resize((100, 100))
                        # Save the resized image to a new BytesIO object
                        buffered = io.BytesIO()
                        img.save(buffered, format="PNG")
                        # Get the new base64 string
                        new_image_base64 = base64.b64encode(buffered.getvalue()).decode()
                    
                    # Convert to Excel cell reference
                    cell = xl_rowcol_to_cell(row_num - 1, image_preview_column)
                    
                    # Insert the resized image
                    worksheet.insert_image(cell, '', {
                        'image_data': io.BytesIO(base64.b64decode(new_image_base64)),
                        'object_position': 1,  # 1 means top left
                        'x_scale': 1,  # No scaling needed as image is already resized
                        'y_scale': 1,  # No scaling needed as image is already resized
                    })
            
            # Set row height and column width for the image preview column
            worksheet.set_default_row(200)  # Set row height to 200 pixels
            worksheet.set_column(image_preview_column, image_preview_column, 20)  # Set column width to 20 units
    
    # Offer the Excel file for download
    excel_data = output.getvalue()
    st.download_button(
        label="Download results as Excel",
        data=excel_data,
        file_name="warehouse_analysis_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    
    # Display a preview of the results
    st.write("Results:")
    # Display the DataFrame without the 'image' column
    st.dataframe(result_df.drop(columns=['image']))

st.title("Warehouse Analyzer from Google Maps")
password = st.text_input("Enter password", type="password")
correct_password = os.getenv("APP_PASSWORD")
if password == correct_password:
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write("Preview of uploaded data:")
        st.write(df)
        
        google_maps_columns = [col for col in df.columns if df[col].astype(str).str.contains(r'^https?://.*google.*maps', case=False, regex=True).any()]
        
        if not google_maps_columns:
            st.error("Error: Cannot find a column containing Google Maps links in the uploaded file.")
        else:
            maps_link_column = st.selectbox("Select the column containing Google Maps links:", google_maps_columns)
            
            if st.button("Preview Results"):
                with st.spinner('Processing preview...'):
                    preview_df = asyncio.run(process_csv(df.head(10), maps_link_column, preview=True))
                
                st.write("Preview of results (first 10 rows):")
                display_preview(preview_df)
                
            if st.button("Process Entire Dataset"):
                with st.spinner('Processing entire dataset... !!Please do not close this tab!!'):
                    result_df = asyncio.run(process_csv(df, maps_link_column))
                
                st.write("Results:")
                display_results(result_df)
else:
    st.error("Incorrect password. Please try again.")



st.markdown("---")
st.markdown("Powered by Openmart")