import json
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import secretmanager
import google.generativeai as genai
import os
import re
import time
from datetime import datetime
from flask import Flask, request

app = Flask(__name__)

# Initialize Firebase Admin with Application Default Credentials
if not firebase_admin._apps:
    firebase_admin.initialize_app()

db = firestore.client()

# Configuration
PROJECT_ID = os.environ.get("PROJECT_ID", "agentic-ai-476923")
SECRET_ID = os.environ.get("SECRET_ID", "Gemini_API_KEY_denuncias")
VERSION_ID = "latest"
SOURCE_COLLECTION = "denuncias"
DEST_COLLECTION = "Síntesis de denuncias"

def get_gemini_api_key():
    """Retrieves the Gemini API key from Google Cloud Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/{VERSION_ID}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def synthesize_and_extract(text, report_date, api_key):
    """Synthesizes text and extracts time using Gemini API."""
    if not api_key:
        return None
    
    # Calculate day of the week
    day_of_week_str = "Desconocido"
    try:
        # Assuming format MM/DD/YYYY
        dt = datetime.strptime(report_date, "%m/%d/%Y")
        days = ["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"]
        day_of_week_str = days[dt.weekday()]
    except:
        pass 
    
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.0-flash')
        
        # Retry logic for 429 errors
        max_retries = 3
        retry_delay = 5  # Initial delay in seconds
        
        for attempt in range(max_retries):
            try:
                prompt = f"""
                Analiza el siguiente texto de una denuncia y extrae la siguiente información en formato JSON:
                1. "sintesis": Un resumen de máximo 300 caracteres.
                2. "tiempo": ¿Cuándo ocurrió? 
                   - Primero, detecta que la "Fecha del reporte" cae en un día: {day_of_week_str}.
                   - Si el texto indica un rango de tiempo (ej. "la semana pasada", "hace dos semanas"), calcula las fechas exactas basándote en la fecha del reporte.
                   - REGLA DE ORO: Si proves un rango, este debe estar delimitado SIEMPRE por una fecha inicial que sea LUNES y una fecha final que sea DOMINGO.
                   - Formato deseado: "MM/DD/AAAA" o "En la semana del DD/MM/AAAA al DD/MM/AAAA". 
                3. "modo": ¿Cómo se ejecutó? Describe la mecánica del presunto soborno o irregularidad: mensajes, reuniones, entregas, correos, etc. Ejemplo: "El técnico presuntamente contactó al gestor vía WhatsApp desde un número privado, citándolo en una cafetería para la entrega del efectivo (USD $500.00)".
                4. "circunstancia": ¿En qué contexto? Indica licitaciones específicas, trámites determinados, lugares, presencia de terceros. Ejemplo: "El hecho ocurre en el marco del trámite de factibilidad de uso de suelo. Se aporta como indicio el número de expediente administrativo que actualmente se encuentra 'estancado' en el escritorio del técnico mencionado."
                5. "alcaldia": ¿En qué alcaldía sucedieron los hechos? Extrae el nombre del municipio o alcaldía. Ejemplo: "San Juan", "PaloAlto".
                6. "es_anonima": ¿Es anónima? Responde "SI" o "NO" basándote en si el usuario solicitó anonimato o no proporcionó su nombre.

                REGLA GENERAL: Si se mencionan montos de dinero en cualquiera de los campos, escríbelos también con números y signo de dinero. Ejemplo: "un millón de dólares (USD $1,000,000.00)".

                Fecha del reporte: {report_date} (Día: {day_of_week_str})
                Texto:
                {text}
                
                Responde ÚNICAMENTE con el JSON válido.
                """
                
                response = model.generate_content(prompt)
                
                # Clean up response to ensure valid JSON
                content = response.text.strip()
                if content.startswith("```json"):
                    content = content[7:]
                if content.endswith("```"):
                    content = content[:-3]
                
                return json.loads(content)
            except Exception as e:
                if "429" in str(e) and attempt < max_retries - 1:
                    print(f"Quota exhausted (429). Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    raise e
    except Exception as e:
        print(f"Error generating content: {e}")
        return {"sintesis": f"Error: {str(e)}", "tiempo": "Error", "modo": "Error", "circunstancia": "Error", "alcaldia": "Error", "es_anonima": "Error"}

@app.route('/', methods=['POST', 'GET'])
def process_denuncias(request_obj=None):
    """HTTP Cloud Function to process all reports."""
    
    if request.method == 'GET':
        return '''
        <h1>Procesador de Denuncias</h1>
        <p>Usa POST para procesar denuncias:</p>
        <code>curl -X POST https://[TU-URL]</code>
        '''
    
    try:
        # 2. Get API Key
        print("Retrieving API Key from Secret Manager...")
        api_key = get_gemini_api_key()
        if not api_key:
            return "Failed to retrieve API key.", 500

        # 3. Fetch Data
        print(f"Fetching documents from collection '{SOURCE_COLLECTION}'...")
        docs = db.collection(SOURCE_COLLECTION).stream()
        
        batch = db.batch()
        batch_count = 0
        processed_count = 0
        
        for doc in docs:
            doc_data = doc.to_dict()
            doc_id = doc.id
            transcript = doc_data.get("Transcript", "")
            
            print(f"Processing document {doc_id}...")
            
            report_date = doc_data.get("Date", "Fecha desconocida")
            
            if transcript:
                extracted_data = synthesize_and_extract(transcript, report_date, api_key)
                if extracted_data:
                    doc_data["Síntesis"] = extracted_data.get("sintesis", "")
                    doc_data["Tiempo"] = extracted_data.get("tiempo", "")
                    doc_data["Modo"] = extracted_data.get("modo", "")
                    doc_data["Circunstancia"] = extracted_data.get("circunstancia", "")
                    doc_data["Alcaldía"] = extracted_data.get("alcaldia", "")
                    
                    es_anonima = extracted_data.get("es_anonima", "NO").upper()
                    doc_data["¿Es anónima?"] = es_anonima
                    
                    if es_anonima == "SI":
                        doc_data["Registro"] = "Aviso"
                    else:
                        doc_data["Registro"] = "Denuncia"
            else:
                doc_data["Síntesis"] = "No Transcript available."
                doc_data["Tiempo"] = "N/A"
                doc_data["Modo"] = "N/A"
                doc_data["Circunstancia"] = "N/A"
                doc_data["Alcaldía"] = "N/A"
                doc_data["¿Es anónima?"] = "N/A"
                doc_data["Registro"] = "N/A"
                
            # Prepare for Firestore upload
            dest_ref = db.collection(DEST_COLLECTION).document(doc_id)
            batch.set(dest_ref, doc_data)
            batch_count += 1
            processed_count += 1
            
            time.sleep(2)
            
            if batch_count >= 400: 
                batch.commit()
                batch = db.batch()
                batch_count = 0
        
            

        # Commit remaining
        if batch_count > 0:
            batch.commit()
        
        return f"✅ Proceso completo. Procesadas {processed_count} denuncias y subidas a la colección '{DEST_COLLECTION}'.", 200
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return f"❌ Error procesando: {str(e)}", 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return 'OK', 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)