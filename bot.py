import logging
from telegram.ext import Updater, CommandHandler
import telegram
import pandas as pd
import logging
import exchSpreads


# Configure Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()

# Solicitar TOKEN
TOKEN = "5674447793:AAGD9mla7hpVUfsE720ivrMvcR47oA6Dveo"

def tracker(update,context):
  chat_id = update.effective_chat['id']
  title = update.effective_chat['title']
  name = update.effective_user['first_name']
  logger.info(
        f"El usuario {name}, ha puesto una solicitud de spreads EXCH en el chat {title} (id = {chat_id} )")
  context.bot.sendMessage(
        chat_id=chat_id, parse_mode="HTML", text="Starting EXCH Spreads for all Coins....")
  exchSpreads.run()

if __name__ == "__main__":
    # Obtenemos la informacion del Bot
    my_bot = telegram.Bot(token=TOKEN)

# Enlazamos nuestro updater con nuestro bot
updater = Updater(my_bot.token, use_context=True)

# Creamos un despachador
dp = updater.dispatcher
                         
 
dp.add_handler(CommandHandler("exch_spread_tracker_all_coins", tracker ))
# Pregunta al bot si hay nuevos msjs
updater.start_polling()
                           
print("BOT CARGADO")
                           
updater.idle()  # finalizar el bot ctrl+c
