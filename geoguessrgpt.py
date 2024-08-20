import io
import streamlit as st
import pandas as pd
import requests
import re
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
        'size': '640x640',
        'maptype': 'satellite',
        'key': api_key
    }
    async with session.get(endpoint, params=params) as response:
        if response.status == 200:
            image_data = await response.read()
            image = Image.open(BytesIO(image_data))
            
            # Convert the image to RGB mode to ensure compatibility with drawing operations
            image = image.convert('RGB')
            
            # Save the image before drawing
            image.save("before_drawing.png")
            print(f"Image size before drawing: {image.size}")
            
            from PIL import ImageDraw, ImageFont
            
            # Add rulers or measures in feet
            draw = ImageDraw.Draw(image)
            font = ImageFont.load_default()
            
            # Calculate scale based on zoom level (approximate)
            if zoom == 19:
                scale = 450 / 640  # ~450ft for 640 pixels at zoom 19
            elif zoom == 18:
                scale = 900 / 640  # ~900ft for 640 pixels at zoom 18
            # Draw white background for rulers
            draw.rectangle([(0, 620), (640, 640)], fill="white")  # Horizontal ruler background
            draw.rectangle([(600, 0), (640, 640)], fill="white")  # Vertical ruler background
            
            # Draw horizontal ruler
            for i in range(int(100 / scale), 630, int(100 / scale)):  # Every 100ft, starting from 100ft
                # Draw a more visible marker
                draw.rectangle([(i+3, 0), (i+4, 640)], fill="black")
                label = f"{round(i * scale / 100) * 100}ft"
                draw.text((i, 630), label, fill="black", font=font, anchor="ms")
            
            # Draw vertical ruler
            for i in range(int(100 / scale), 630, int(100 / scale)):  # Every 100ft, starting from 100ft
                # Draw a more visible marker
                draw.rectangle([(0, i+2), (640, i+3)], fill="black")
                label = f"{round(i * scale / 100) * 100}ft"
                draw.text((625, i), label, fill="black", font=font, anchor="rm")

            # Add a text box pointing to the center of map
            text = "Estimate this building"
            text_bbox = draw.textbbox((0, 0), text, font=font)
            text_width = text_bbox[2] - text_bbox[0]
            text_height = text_bbox[3] - text_bbox[1]
            
            # Calculate position for text box (top-left corner)
            text_position = (320 - text_width // 2, 280)  # Moved closer to the center
            
            # Draw white background for text
            padding = 2
            draw.rectangle([
                (text_position[0] - padding, text_position[1] - padding),
                (text_position[0] + text_width + padding, text_position[1] + text_height + padding)
            ], fill="white", outline="black")
            
            # Draw text
            draw.text(text_position, text, fill="black", font=font)
            
            # Draw red arrow pointing to the center
            arrow_start = (320, text_position[1] + text_height + padding + 5)
            arrow_end = (320, 320)  # Center of the image
            draw.line([arrow_start, arrow_end], fill="red", width=2)
            
            # Draw red arrowhead
            arrowhead_size = 10
            draw.polygon([
                (arrow_end[0], arrow_end[1]),
                (arrow_end[0] - arrowhead_size, arrow_end[1] - arrowhead_size),
                (arrow_end[0] + arrowhead_size, arrow_end[1] - arrowhead_size)
            ], fill="red")

            # Save the image after drawing
            image.save("after_drawing.png")
            print(f"Image size after drawing: {image.size}")
            return image
        else:
            raise Exception(f"Error fetching image: {response.status} - {await response.text()}")

# Test the function
# async def test_get_google_satellite_image():
#     test_lat, test_lon = 37.7749, -122.4194  # Example coordinates (San Francisco)
#     test_zoom = 19

#     async with aiohttp.ClientSession() as session:
#         try:
#             image = await get_google_satellite_image(session, test_lat, test_lon, google_maps_api_key, test_zoom)
#             print("Image successfully retrieved and processed.")
#             # Optionally, you can save the image to verify it visually
#             image.save("test_satellite_image.png")
#             print("Image saved as 'test_satellite_image.png'")
#         except Exception as e:
#             print(f"Error during test: {e}")

# # Run the test function
# if __name__ == "__main__":
#     asyncio.run(test_get_google_satellite_image())

def image_to_base64(image):
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode()

class GoogleSatelliteAnalysis(BaseModel):
    calculations: str = Field(description="Explain your calculations for each field from the images provided, how many ruler unit the warehouse takes up.")
    is_warehouse: bool = Field(description="From the satellite images, does this building complex look like a warehouse. Explanation: Look for rectangular buildings with loading docks, truck parking areas, and large open spaces around the structure.")
    warehouse_length: Optional[int] = Field(default=None, description="The approximate length of the warehouse in feet. Explanation: Estimate the longer side of the building. Use known objects like trucks or based on the size of the image for scale.", example=900)
    warehouse_width: Optional[int] = Field(default=None, description="The approximate width of the warehouse in feet. Explanation: Estimate the shorter side of the building. Use known objects like trucks or based on the size of the image for scale.", example=500)
    num_loading_docks: Optional[int] = Field(description="How many loading docks are there, if any. Look for loading docks around the warehouse")
    num_trucks_on_dock: Optional[int] = Field(description="How many trucks are on the loading dock, if any.")
    warehouse_size_confidence: Optional[int] = Field(default="How confident are you in your estimation? rate it 0-10, e.g. incomplete image, not enough info can contribute to a low score")

async def analyze_image(image_base64_zoom19, image_base64_zoom18):
    response = await client.chat.completions.create(
        model="gpt-4o",
        response_model=GoogleSatelliteAnalysis,
        messages=[
            {
                "role": "system",
                "content": "You are an satellite image engineer and expert statistician."
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Analyze the entire building complex (anything that is connected to the structure) marked on the satellite image. Is this entire building complex a warehouse? If so, provide details about its size based on the ruler on the image. If it's not a warehouse or you're unsure, indicate that in your response. Be very accurate with your estimation."},
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

async def process_batch(batch, session, lat_column, lon_column):
    tasks = []
    for row in batch:
        lat = row[lat_column]
        lon = row[lon_column]
        if pd.notna(lat) and pd.notna(lon):
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
                'image': image_base64_zoom18,
                'is_warehouse': analysis.is_warehouse,
                'warehouse_length': analysis.warehouse_length,
                'warehouse_width': analysis.warehouse_width,
                'num_loading_docks': analysis.num_loading_docks,
                'num_trucks_on_dock': analysis.num_trucks_on_dock,
                'calculations': analysis.calculations,
                'warehouse_size_confidence': analysis.warehouse_size_confidence
            }
        else:
            st.warning(f"Analysis failed for row: {row}")
            return None
    except Exception as e:
        st.error(f"Error processing row: {str(e)}")
        return None

async def process_csv(df, lat_column, lon_column, preview=False):
    results = []
    total_rows = len(df) if not preview else min(10, len(df))
    progress_bar = st.progress(0)
    progress_bar.max = total_rows
    current_progress = 0
    progress_text = st.empty()
    
    async with aiohttp.ClientSession() as session:
        for i in range(0, total_rows, ASYNC_BATCH_SIZE):
            batch = df.iloc[i:min(i+ASYNC_BATCH_SIZE, total_rows)].to_dict('records')
            batch_results = await process_batch(batch, session, lat_column, lon_column)
            current_progress += len(batch_results) / float(total_rows)
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
    int_columns = ['warehouse_width', 'warehouse_length']
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
                        # Resize the image to 300x300
                        img = img.resize((300, 300))
                        # Convert to RGB mode if it's not already
                        if img.mode != 'RGB':
                            img = img.convert('RGB')
                        # Save the resized image to a new BytesIO object with compression
                        buffered = io.BytesIO()
                        img.save(buffered, format="JPEG", quality=85, optimize=True)
                        # Get the new base64 string
                        new_image_base64 = base64.b64encode(buffered.getvalue()).decode()
                    
                    # Convert to Excel cell reference
                    cell = xl_rowcol_to_cell(row_num - 1, image_preview_column)
                    
                    # Insert the compressed image
                    worksheet.insert_image(cell, '', {
                        'image_data': io.BytesIO(base64.b64decode(new_image_base64)),
                        'object_position': 1,  # 1 means top left
                        'x_scale': 1,  # No scaling needed as image is already resized
                        'y_scale': 1,  # No scaling needed as image is already resized
                    })
            # Set row height and column width for the image preview column
            worksheet.set_default_row(300)  # Set row height to 300 pixels
            worksheet.set_column(image_preview_column, image_preview_column, 30)  # Set column width to 30 units
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
        
        input_type = st.radio("Select input type:", ["Google Maps Link", "Latitude and Longitude"])
        
        if input_type == "Google Maps Link":
            google_maps_columns = [col for col in df.columns if df[col].astype(str).str.contains(r'^https?://.*google.*maps', case=False, regex=True).any()]
            
            if not google_maps_columns:
                st.error("Error: Cannot find a column containing Google Maps links in the uploaded file.")
            else:
                maps_link_column = st.selectbox("Select the column containing Google Maps links:", google_maps_columns)
                
                def extract_lat_lon(url):
                    match = re.search(r'!3d([-\d.]+)!4d([-\d.]+)', url)
                    if match:
                        return float(match.group(1)), float(match.group(2))
                    return None, None

                df['latitude'], df['longitude'] = zip(*df[maps_link_column].apply(extract_lat_lon))
                
                if st.button("Preview Results"):
                    with st.spinner('Processing preview...'):
                        preview_df = asyncio.run(process_csv(df.head(10), 'latitude', 'longitude', preview=True))
                    
                    st.write("Preview of results (first 10 rows):")
                    display_preview(preview_df)
                    
                if st.button("Process Entire Dataset"):
                    with st.spinner('Processing entire dataset... !!Please do not close this tab!!'):
                        result_df = asyncio.run(process_csv(df, 'latitude', 'longitude'))
                    
                    st.write("Results:")
                    display_results(result_df)
        else:
            lat_column = st.selectbox("Select the column containing latitude:", df.columns)
            lon_column = st.selectbox("Select the column containing longitude:", df.columns)
            
            if st.button("Preview Results"):
                with st.spinner('Processing preview...'):
                    preview_df = asyncio.run(process_csv(df.head(10), lat_column, lon_column, preview=True))
                
                st.write("Preview of results (first 10 rows):")
                display_preview(preview_df)
                
            if st.button("Process Entire Dataset"):
                with st.spinner('Processing entire dataset... !!Please do not close this tab!!'):
                    result_df = asyncio.run(process_csv(df, lat_column, lon_column))
                
                st.write("Results:")
                display_results(result_df)
else:
    st.error("Incorrect password. Please try again.")



st.markdown("---")
st.markdown("Powered by Openmart")