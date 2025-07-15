import uuid
import random
from datetime import datetime
from typing import Dict, Any

class MessageGenerator:
    def __init__(self):
        self.categories = [
            "menu_variety", "taste_quality", "service_speed", 
            "ambiance", "price_performance"
        ]
        self.sentiments = ["positive", "negative", "neutral"]
        self.languages = ["tr", "en"]
    
    def generate_comment_message(self) -> Dict[str, Any]:
        """Generate a random comment message"""
        return {
            "commentId": str(uuid.uuid4()),
            "text": self._generate_comment_text(),
            "timestamp": datetime.now().isoformat(),
            "category": random.choice(self.categories),
            "language": random.choice(self.languages),
            "sentiment": random.choice(self.sentiments)
        }
    
    def _generate_comment_text(self) -> str:
        """Generate realistic comment text"""
        comments = [
            "Harika bir deneyimdi, kesinlikle tekrar geleceğim!",
            "Yemekler çok lezzetliydi, servis de hızlıydı.",
            "Fiyatlar biraz yüksek ama kalite iyi.",
            "Ortam çok güzeldi, romantik bir akşam yemeği için ideal.",
            "Menüde çok fazla seçenek var, herkes için bir şey bulunur.",
            "Servis biraz yavaştı ama yemekler değerdi.",
            "Temizlik konusunda daha dikkatli olabilirler.",
            "Personel çok nazik ve yardımseverdi.",
            "Porsiyonlar küçük ama lezzetliydi.",
            "Rezervasyon yapmak gerekiyor, çok kalabalık."
        ]
        return random.choice(comments)
    
    def generate_chat_message(self, user_id: str, message: str, memory_type: str) -> Dict[str, Any]:
        """Generate chat message for Kafka"""
        return {
            "user_id": user_id,
            "message": message,
            "memory_type": memory_type,
            "timestamp": datetime.now().isoformat(),
            "message_id": str(uuid.uuid4())
        } 