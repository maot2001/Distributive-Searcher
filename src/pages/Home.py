import streamlit as st

client = st.session_state.client
if 'edit' in st.session_state:
    edit = st.session_state.edit
    client.edit(edit[0], edit[1])
    del st.session_state['edit']

if 'remove' in st.session_state:
    client.remove(st.session_state.remove)
    del st.session_state['remove']

st.title('Dis-Gle')
with st.form(key='search_form'):
    query = st.text_input("Buscar:", "")
    submit_button = st.form_submit_button(label="Buscar")

if submit_button:
    st.session_state.query = query
    st.switch_page("pages/Query.py")

uploaded_file = st.sidebar.file_uploader("Subir archivo .txt", type=['txt'])

if uploaded_file is not None:
    client.insert(uploaded_file.getvalue().decode())
    