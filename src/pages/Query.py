import streamlit as st

client = st.session_state.client

if 'query' in st.session_state:
    query = st.session_state.query
    responses = client.search(query)
    data = responses.decode().split('&&&')
    docs = []
    for d in data:
        if d == '': continue
        id = d.split(',')[0]
        text = ','.join(d.split(',')[1:])
        docs.append((id, text))
    st.session_state.docs = docs
    del st.session_state['query']
else:
    docs = st.session_state.docs
    
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

for i, r in enumerate(docs):
    st.write(r[1])
    if st.button('Ver Más...', key=f'show_{i}'):
        doc = client.get(r[0])
        data = doc.decode().split(',')
        doc = ','.join(data[0:])
        st.session_state.doc = (r[0], doc)
        del st.session_state['docs']
        st.switch_page("pages/Document.py")