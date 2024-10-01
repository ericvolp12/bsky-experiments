import logging
import os
import time
import torch
from opentelemetry import trace
from transformers import T5Tokenizer, T5ForConditionalGeneration


logger = logging.getLogger(__name__)


MODEL = f"google-t5/t5-small"

tokenizer = T5Tokenizer.from_pretrained(MODEL)

device = "cuda:0" if torch.cuda.is_available() else "cpu"

model = T5ForConditionalGeneration.from_pretrained(MODEL).to(device)

model.save_pretrained(f"{os.getcwd()}/{MODEL}")

tracer = trace.get_tracer("translate")

def translate_text(input: str, src_lang: str, target_language: str) -> str:
    with tracer.start_as_current_span("translate") as span:
        start = time.time()
        prefix = f"translate {src_lang} to {target_language}: "

        text_inputs = tokenizer(prefix+input, return_tensors="pt").to(device)

        with tracer.start_as_current_span("model_inference"):
            output_tokens = model.generate(**text_inputs, max_new_tokens=60)
            output_text = tokenizer.decode(output_tokens[0], skip_special_tokens=True)
        end = time.time()
        logger.info("translated", extra={"input": input, "output": output_text, "time": end - start, "src_lang": src_lang, "target_language": target_language})
        return output_text
