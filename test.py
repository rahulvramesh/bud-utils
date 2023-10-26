import os
from dotenv import load_dotenv
from bud_ecosystem_utils.blob import BlobService
from bud_ecosystem_utils.data_utils import BudMLOpsClient, download_dataset

load_dotenv("/home/azureuser/bud-ecosystem-core/nodes/train-stable-diffusion-lora/.env")

session_id = "0000"
node_id = "test"
ckpt_path = "/home/azureuser/TorchTrainer_0bb29_00000_0_2023-10-25_07-42-27"
output_dir = "sd-model-finetuned-lora"
# model = "34bd9e07-c240-4853-a477-bedecece9a99"
model = ""

blob_service = BlobService()
base_key = f"{session_id}/{node_id}"
filename = blob_service.get_unique_key()
trainer_states_url = blob_service.bulk_upload(
    ckpt_path,
    base_key + "/ray-trainer/" + filename + ".zip",
    exclusions=[output_dir],
    zip_data=True,
)
model_weights_url = blob_service.bulk_upload(
    os.path.join(ckpt_path, output_dir), base_key + "/weights/" + filename + ".zip", zip_data=True
)
print("[INFO] States and Weights saved to blob")

output = {
    "model_weights_uri": model_weights_url,
    "trainer_states_uri": trainer_states_url,
}

if model:
    bud_mlops_client = BudMLOpsClient()
    resp = bud_mlops_client.register_model(
        filename + ".zip", 
        model_weights_url, 
        "adapter", 
        "sd1_5", 
        model
    )
    print("[INFO] Model registered to Bud MLOps backend")
    print(resp)
    output["extras"] = {
        "model_id": resp["model_id"]
    }

print(output)


data_savepath, _ = download_dataset(output["model_weights_uri"])
try:
    import shutil
    shutil.rmtree(data_savepath)
except Exception as e:
    print(f"[ERROR] Failed deleting dataset and model file, e -> {e}")