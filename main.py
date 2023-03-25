from __future__ import annotations
from typing import Tuple, Any, Callable, List, Union, Dict

import re
from datetime import datetime
import json
import streamlit as st

import pandas as pd
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark import Session, FileOperation, Row
from snowflake.snowpark.functions import col
import snowflake.snowpark.types as T
from snowflake.snowpark.types import StringType
import snowflake.snowpark.functions as F

from streamlit_helpers import Utils

from enum import Enum, IntEnum
import base64

from io import StringIO, BytesIO
import hashlib

####################################
#
# Connection management
# 
####################################

@st.cache_data
def get_connection_details() -> Row:
    if session:
        try:
            connInfo = session.sql("""
                SELECT
                    current_user() as "user"
                    , current_role() as "role"
                    , current_account() as "account"
                    , current_warehouse() as "wh"
                    , current_database() as "db"
                    , current_schema() as "schema"   
                """).collect()
            if len(connInfo) > 0:
                return connInfo[0]
        except SnowparkSQLException as e:            
            st.cache_resource.clear()
            st.experimental_rerun()
    return None

@st.cache_data
def get_warehouses() -> List[Row]:
    if session:
        warehouses = session.sql('SHOW WAREHOUSES;').collect();
        return [_.name for _ in warehouses]
    return []

@st.cache_resource
def connect() -> Session:
    _session = None
    print('Connecting')

    try:
        print('Trying SiS')
        from snowflake.snowpark.context import get_active_session
        _session = get_active_session()
        if not 'is_sis' in st.session_state:
            st.session_state.is_sis = ('connection' not in st.session_state)
    except:
        print('Trying state')
        if 'connection' in st.session_state:
            print('... found it')
            _session = Session.builder.configs(st.session_state.connection).create()

    return _session

def use_warehouse(warehouse_name:str) -> None:
    if session:
        session.use_warehouse(warehouse_name)
        st.write(session.get_current_warehouse())

def clear_cache_and_rerun():
    st.experimental_singleton.clear()
    st.experimental_memo.clear()
    st.experimental_rerun()

@st.cache_data
def load_stages() -> List[Any]:
    if session and session.get_current_warehouse():
        stages = session.sql('SHOW STAGES;').collect();
        
        streamlit_stages = [{"fullname":f'@{_.database_name}.{_.schema_name}."{_.name}"', "name": _.name[:-(len(' (Stage)'))]} for _ in stages if _.name.endswith('(Stage)')]
        return streamlit_stages
    return None

def load_stage_content(stage_fullname:str) -> List[Any]:
    if session and session.get_current_warehouse():

        content_names = session.sql(f'LIST \'{stage_fullname}\'').collect()

        stage_name = stage_fullname.split('.')[-1:][0].strip('"')

        content = [{"file_name": _[0][len(stage_name)+1:], "stage_name":stage_name, "fullname":f'{stage_fullname}/{_[0][len(stage_name)+1:]}', "size":_[1], "stage_fullname": stage_fullname} for _ in content_names]
        return content

def load_content(content_fullname:str) -> bytes:
    if session and session.get_current_warehouse():
        fd = session.file.get_stream(content_fullname)
        with fd:
            bytes = fd.read()

        return bytes

def save_content(content_full_name:str, content_bytes:bytes) -> None:
    stream = BytesIO(content_bytes)
    if session and session.get_current_warehouse():
        session.file.put_stream(stream, content_full_name, overwrite=True, auto_compress=False)

def delete_content(content_full_name:str) -> None:
    if session and session.get_current_warehouse():
        print(f'DO THIS: --- REMOVE \'{content_full_name}\';')
        session.sql(f'REMOVE \'{content_full_name}\';').collect()

def show_content(content_name:str, content_bytes:bytes) -> None:

    file_extension = content_name.split('.')[-1:][0].lower()

    if file_extension == 'py':

        st.caption('Python code found')

        py_code = content_bytes.decode('utf-8')
        with st.expander('Show code'):
            st.code(py_code)
        return

    if file_extension == 'svg':

        st.caption('SVG image found')
        mime_type = 'svg+xml'

        content_b64encoded = base64.b64encode(content_bytes).decode()
        image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
        st.image(image_string)

        return

    if file_extension == 'png':

        st.caption('PNG image found')
        mime_type = 'png'

        content_b64encoded = base64.b64encode(content_bytes).decode()
        image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
        st.image(image_string)


        return
    
    if file_extension in ('jpg', 'jpeg'):

        st.caption('JPEG image found')
        mime_type = 'jpeg'

        content_b64encoded = base64.b64encode(content_bytes).decode()
        image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
        st.image(image_string)


        return
    
    st.write(bytes)

def show_login_view():

    login_view_empty = st.empty()

    with login_view_empty:

        c1,c2,c3 = st.columns([1,3,1])
        with c2:

            try:
                secrets = st.secrets
                secrets_keys = [s for s in secrets]
                has_secrets = len(secrets_keys) > 0
            except:
                secrets_keys = []
                secrets = {}
                has_secrets = False
            
            tab_user_password, tab_saved_connection = st.tabs(['Directly with user/password', 'Saved connections'])

            if has_secrets:
                # Connect with saved account
                with tab_saved_connection:
                    st.caption('Connect with saved connection data:')

                    connection_name = st.selectbox('Select a saved connection', [''] + secrets_keys, key='connection_name')

                    connect_with_name = st.button('Connect', key='button_connect_using_saved_connection')

                    if connect_with_name:
                        print(f'{connection_name} selected')
                        connection_params = secrets[connection_name]
                        st.session_state.connection = connection_params
                        print(st.session_state.connection)
                        clear_cache_and_rerun()

            # Connect with user/password
            with tab_user_password:
                st.caption('Connect directly with user name and password:')
                connection_locator = st.text_input('Account locator:', key='connection_locator')
                connection_user = st.text_input('User:', key='connection_user')
                connection_password = st.text_input('Password:', type='password', key='connection_password')
                    
                connect_with_details = st.button('Connect', key='button_connect_using_user_password')

                if connect_with_details:
                    print(f'{connection_locator} selected + {connection_user}/{connection_password}')
                    connection_params = {
                            'account':connection_locator,
                            'user':connection_user,
                            'password':connection_password
                        }
                    st.session_state.connection = connection_params
                    print(st.session_state.connection)
                    clear_cache_and_rerun()

def show_side_bar():
    with st.sidebar:
        st.metric('Time reloaded', datetime.now().strftime('%H:%M:%S'))
        st.metric('Connected to', session.get_current_account().strip('"') if session != None else "No")

        if session:
            connection_details = get_connection_details()
            #st.write(connection_details)

            current_warehouse = session.get_current_warehouse()
            if current_warehouse == None:
                warehouses = get_warehouses()
                warehouse = st.selectbox('Select a warehouse', warehouses)
                if warehouse:
                    use_warehouse(warehouse)
            else:
                st.metric('Warehouse', current_warehouse.strip('"'))

            stages = load_stages()
            selected_stage_name = st.selectbox('Select a Streamlit app', [_["name"] for _ in stages])
            if selected_stage_name:                
                selected_stage = [_ for _ in stages if _["name"]==selected_stage_name][0]

            if selected_stage:
                st.session_state.selected_stage_fullname = selected_stage["fullname"]
                st.subheader(f'Staged files for: {selected_stage["name"]}')
                contents = load_stage_content(selected_stage["fullname"])

                all_files =  [_["file_name"] for _ in contents]
                all_files.extend(["new..."])

                selected_index = len(all_files)-1
                if 'new_value_added' in st.session_state:
                    selected_index = all_files.index(st.session_state.new_value_added)
                    st.session_state.pop('new_value_added')
                st.session_state.selected_file_name = st.radio("Choose file", all_files, index=selected_index)

                if st.session_state.selected_file_name == "new...":
                    st.session_state.selected_content = None
                    st.session_state.pop('selected_content')
                else:
                    st.session_state.selected_content = [_ for _ in contents if _["file_name"] ==  st.session_state.selected_file_name][0]

            if 'selected_content' in st.session_state:
                #selected_content = [_ for _ in contents if _["file_name"] ==  st.session_state.selected_content][0]
                st.metric('Selected content', st.session_state.selected_content["file_name"])

            st.markdown('---')
            disconnect = st.button('Disconnect', key='button_logout', disabled=is_sis)
            if disconnect:
                print('Disconnecting')
                session.close()    
                st.experimental_singleton.clear()    
                st.experimental_memo.clear()
                st.session_state.clear()
                st.experimental_rerun()    

def show_content_edit():
    if session and 'selected_content' in st.session_state:
        content = load_content(st.session_state.selected_content["fullname"])

        t1, t2 = st.tabs(('File details', 'Content'))
        with t1:
            for _ in st.session_state.selected_content:
                st.caption(_)
                st.write(st.session_state.selected_content[_])

            if not st.session_state.selected_content["file_name"] == 'streamlit_app.py':

                print(st.session_state)
                delete_file_key = f'delete_{hashlib.sha1(st.session_state.selected_content["fullname"].encode()).hexdigest()}'
                confirm_delete_file = st.checkbox(f'Confirm delete of file {st.session_state.selected_content["fullname"]}')
                delete_file = st.button('Delete file', key=delete_file_key, disabled=(not confirm_delete_file))
                if delete_file and confirm_delete_file:
                    delete_content(st.session_state.selected_content["fullname"])
                    st.session_state.selected_content = None
                    st.session_state.pop('selected_content')
                    st.experimental_rerun()                     

        with t2:
            show_content(st.session_state.selected_content["file_name"], content)

def show_content_new():

    if session and not 'selected_content' in st.session_state:

        t1, t2, t3 = st.tabs(['Image file upload', 'SVG paste', 'Python code paste'])
        with t1:
            uploaded_file = st.file_uploader("Choose a file")
            if uploaded_file is not None:
                # To read file as bytes:
                content_bytes = uploaded_file.getvalue()

                file_name:str = uploaded_file.name
                file_type = file_name.split('.')[-1:][0]
                mime_type = 'application/octet-stream'
                if file_name.endswith('svg'):
                    content_string = content_bytes.decode("utf-8")
                    content_string = Utils.sanitize_svg_for_streamlit(content_string)
                    content_bytes = content_string.encode()
                    mime_type = 'svg+xml'

                if file_name.endswith('png'):
                    mime_type = 'png'

                if file_name.endswith('jpeg') or file_name.endswith('jpg'):
                    mime_type = 'jpeg'

                st.caption("Raw content")

                content_b64encoded = base64.b64encode(content_bytes).decode()
                st.caption("b64 encoded content")

                st.caption("Decoded b64")
                image_string = f'data:image/{mime_type};base64,{content_b64encoded}'
                st.image(image_string)
                st.caption("Copy this into SiS code base")
                st.code(image_string)

                st.caption("Save file to stage")
                save_filename = st.text_input(f'Filename (.{file_type})', value=file_name)

                content_full_name = f'{st.session_state.selected_stage_fullname}/{save_filename}'
                st.write(content_full_name)
                save_content_button = st.button('Save file', disabled=(save_filename==''), key=f'save_content_upload')

                if save_content_button:
                    save_content(content_full_name, content_bytes)
                    stage_name = st.session_state.selected_stage_fullname.split('.')[-1:][0].strip('"')
                    st.session_state.selected_file_name = save_filename
                    st.session_state.selected_content = {
                        "file_name": save_filename, 
                        "stage_name": stage_name, 
                        "fullname": content_full_name, 
                        "size":len(content_bytes), 
                        "stage_fullname": st.session_state.selected_stage_fullname}
                    st.session_state.new_value_added = save_filename
                    st.experimental_rerun()

        with t2:
            svg_data = st.text_area('Paste SVG code')
            
            content_string = Utils.sanitize_svg_for_streamlit(svg_data)

            content_bytes = content_string.encode()
            mime_type = 'svg+xml'

            content_b64encoded = base64.b64encode(content_bytes).decode()
            image_string = f'data:image/{mime_type};base64,{content_b64encoded}'

            if content_string != '':
                st.image(image_string)
                st.caption("Copy this into SiS code base")
                st.code(image_string)

        with t3:
            file_type = 'py'
            py_data = st.text_area('Paste Python code')
            
            content_bytes = py_data.encode()

            st.caption("Save file to stage")
            save_filename = st.text_input(f'Filename (.{file_type})')

            content_full_name = f'{st.session_state.selected_stage_fullname}/{save_filename}'
            st.write(content_full_name)
            save_content_button = st.button('Save file', disabled=(save_filename==''), key=f'save_content_paste_py')

            if save_content_button:
                save_content(content_full_name, content_bytes)
                stage_name = st.session_state.selected_stage_fullname.split('.')[-1:][0].strip('"')
                st.session_state.selected_content = {"file_name": save_filename, "stage_name": stage_name, "fullname": content_full_name, "size":len(content_bytes), "stage_fullname": st.session_state.selected_stage_fullname}
                st.experimental_rerun()


print('New run - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ')
print(st.session_state)

# Are we running in Sis?
is_sis = st.session_state.is_sis if 'is_sis' in  st.session_state else False
session = connect()

if not session:
    show_login_view()

show_side_bar()

show_content_edit()
show_content_new()

