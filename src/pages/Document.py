import streamlit as st

GET_CLIENT = 21
INSERT_CLIENT = 22
REMOVE_CLIENT = 23
EDIT_CLIENT = 24

client = st.session_state.client
doc = st.session_state.doc

uploaded_file = st.sidebar.file_uploader("Subir archivo .txt", type=['txt'])

if uploaded_file is not None:
    client.connect._send_data(INSERT_CLIENT, f'{uploaded_file.getvalue().decode()}')
    uploaded_file = None

st.title('Dis-Gle')
with st.form(key='search_form'):
    query = st.text_input("Buscar:", "")
    submit_button = st.form_submit_button(label="Buscar")

if submit_button:
    st.session_state.query = query
    st.page_link("pages/Query.py", label="GO")

edit_text = st.text_input(doc[1])
if st.button('Editar'):
    client.connect._send_data(EDIT_CLIENT, f'{doc[0]},{edit_text}')
    st.page_link("pages/Home.py", label="GO")

if st.button('Eliminar'):
    client.connect._send_data(REMOVE_CLIENT, f'{doc[0]}')
    st.page_link("pages/Home.py", label="GO")