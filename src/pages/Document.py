import streamlit as st

client = st.session_state.client
if 'doc' in st.session_state:
    doc = st.session_state.doc

uploaded_file = st.sidebar.file_uploader("Subir archivo .txt", type=['txt'])

if uploaded_file is not None:
    client.insert(uploaded_file.getvalue().decode())
    uploaded_file = None

st.title('Dis-Gle')
with st.form(key='search_form'):
    query = st.text_input("Buscar:", "")
    submit_button = st.form_submit_button(label="Buscar")

if submit_button:
    st.session_state.query = query
    st.switch_page("pages/Query.py")

edit_text = st.text_input(doc[1])
if st.button('Editar'):
    st.session_state.edit = (doc[0], edit_text)
    del st.session_state['doc']
    st.switch_page("pages/Home.py")

if st.button('Eliminar'):
    st.session_state.remove = doc[0]
    del st.session_state['doc']
    st.switch_page("pages/Home.py")