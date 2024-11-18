def send_long_message(bot, chat_id, m):
    if len(m) > 4095:
        parts = []
        lines = m.split('\n')
        current_part = ''
        
        for line in lines:
            if len(current_part) + len(line) + 1 <= 4095:  # +1 для \n
                current_part += line + '\n'
            else:
                if current_part:
                    parts.append(current_part)
                current_part = line + '\n'
        
        if current_part:  # Добавляем последнюю часть
            parts.append(current_part)
            
        for part in parts:
            # bot.reply_to(message, text=part, parse_mode="HTML")
            bot.send_message(chat_id, text=part, parse_mode="HTML")
    else:
        # bot.reply_to(message, text=m, parse_mode="HTML")
        bot.send_message(chat_id, text=m, parse_mode="HTML")