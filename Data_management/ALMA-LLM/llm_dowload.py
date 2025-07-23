import os
import torch
from transformers import AutoTokenizer, BitsAndBytesConfig, pipeline, AutoModelForSeq2SeqLM


HUGGINGFACE_TOKEN = os.environ.get("HUGGIN_FACE_API_KEY")
MODEL_ID = "haoranxu/ALMA-7B"

tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, use_auth_token=HUGGINGFACE_TOKEN, use_fast=False )
bnb_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_compute_dtype=torch.float16,  # o torch.bfloat16 si tu GPU lo soporta
    bnb_4bit_use_double_quant=True,        # opcional: mejor equilibrio precisión/tamaño
)


model = AutoModelForSeq2SeqLM.from_pretrained(
    MODEL_ID,
    use_auth_token=HUGGINGFACE_TOKEN,
    quantization_config=bnb_config,
    device_map="auto"
)

translator = pipeline(
    "text2text-generation",
    model=model,
    tokenizer=tokenizer,
    max_length=1000,
    device_map="auto"   # o device=0 si quieres forzar GPU 0
)

out = translator("Translate the following text to English:\n\n这是一个测试")
print(out[0]["generated_text"])
