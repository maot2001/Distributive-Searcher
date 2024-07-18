import streamlit as st

SEARCH_CLIENT = 25
GET_CLIENT = 21
INSERT_CLIENT = 22
REMOVE_CLIENT = 23
EDIT_CLIENT = 24

client = st.session_state.client
query = st.session_state.query
responses = client.connect._send_data(SEARCH_CLIENT, query)
data = responses.decode().split('&&&')
docs = []
for i in range(0, len(data), 2):
    docs.append((data[i], data[i+1]))

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

for i, r in enumerate(docs):
    st.write(r[1])
    if st.button('Ver Más...', key=f'show_{i}'):
        del st.session_state['query']
        doc = client.connect._send_data(GET_CLIENT, r[0])
        data = doc.decode().split(',')
        doc = ','.join(data[0:])
        st.session_state.doc = (r[0], doc)
        st.page_link("pages/Document.py", label="GO")
