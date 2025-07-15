import asyncio
import random
import json
import logging
from datetime import datetime
from typing import Dict, Any, List
import google.generativeai as genai
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CommentGenerator:
    """LLM tabanlı otomatik yorum üretici"""
    
    def __init__(self):
        self.genai = genai
        # API key'i doğrudan kod içinde tanımla
        api_key = ""
        logger.info("Initializing Gemini LLM...")
        print(f"Gemini API Key: {api_key[:6]}...")  # güvenlik için sadece ilk 6 karakter
        if api_key:
            self.genai.configure(api_key=api_key)
            self.model = self.genai.GenerativeModel('gemini-2.5-pro')
            self.use_llm = True
            logger.info("Gemini LLM initialized successfully")
        else:
            logger.warning("GOOGLE_API_KEY not found, using fallback comments")
            self.use_llm = False
        
        # Kafka producer
        self.kafka_producer = None
        
        # Debug environment variables
        logger.info("=== KAFKA ENVIRONMENT DEBUG ===")
        logger.info(f"KAFKA_BOOTSTRAP_SERVERS env: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
        logger.info(f"All environment variables: {dict(os.environ)}")
        
        # Force the correct bootstrap servers
        self.bootstrap_servers = 'kafka:9092'  # Force the correct address
        logger.info(f"Using bootstrap servers: {self.bootstrap_servers}")
        
        # Yorum kategorileri ve duyguları
        self.categories = [
            "menu_variety", "taste_quality", "service_speed", 
            "ambiance", "price_performance"
        ]
        
        self.sentiments = ["positive", "negative", "neutral"]
        
        # Tekrar eden yorumlar için
        self.repeated_comments = []
        self.repeat_probability = 0.15  # %15 tekrar oranı
        
    def connect_kafka(self):
        """Kafka producer bağlantısı"""
        try:
            logger.info(f"Attempting to connect to Kafka with bootstrap servers: {self.bootstrap_servers}")
            logger.info(f"Environment KAFKA_BOOTSTRAP_SERVERS: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
            
            # Force the correct bootstrap servers
            bootstrap_servers = self.bootstrap_servers
            if 'localhost' in bootstrap_servers:
                bootstrap_servers = 'kafka:9092'
                logger.warning(f"Detected localhost in bootstrap servers, forcing to: {bootstrap_servers}")
            
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Kafka producer connected successfully to: {bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect Kafka producer: {e}")
            raise
    
    def generate_comment_prompt(self, category: str, sentiment: str) -> str:
        """Yorum üretimi için prompt oluştur"""
        category_names = {
            "menu_variety": "menü çeşitliliği",
            "taste_quality": "lezzet kalitesi", 
            "service_speed": "servis hızı",
            "ambiance": "ortam ve ambiyans",
            "price_performance": "fiyat/performans"
        }
        
        sentiment_names = {
            "positive": "olumlu",
            "negative": "olumsuz", 
            "neutral": "tarafsız"
        }
        
        # Rastgele stil ve ton seçenekleri
        styles = [
            "samimi ve arkadaşça bir tonda",
            "resmi ve profesyonel bir tonda", 
            "heyecanlı ve coşkulu bir tonda",
            "sakin ve düşünceli bir tonda",
            "mizahi ve eğlenceli bir tonda"
        ]
        
        # Rastgele detay seçenekleri
        details = [
            "spesifik yemek isimleri kullanarak",
            "fiyat bilgisi ekleyerek",
            "personel isimleri kullanarak",
            "zaman bilgisi ekleyerek",
            "mekan detayları belirterek"
        ]
        
        # Rastgele stil ve detay seç
        style = random.choice(styles)
        detail = random.choice(details)
        
        prompt = f"""
        Kategori: {category_names[category]}
        Duygu: {sentiment_names[sentiment]}
        Stil: {style}
        Detay: {detail}
        
        Bu kriterlere uygun, tamamen farklı ve özgün bir müşteri yorumu üret (Türkçe):
        - Gerçekçi ve doğal bir yorum olmalı
        - 5-10 kelime arasında olmalı
        - Belirtilen kategori ve duyguya uygun olmalı
        - Restoran deneyimi hakkında olmalı
        - Her seferinde farklı bir yorum olmalı
        - Belirtilen stil ve detay kriterlerine uygun olmalı
        
        Sadece yorum metnini döndür, başka açıklama ekleme.
        """
        
        return prompt
    
    async def generate_comment(self, category: str, sentiment: str) -> str:
        """LLM ile yorum üret"""
        if not self.use_llm:
            return self._get_fallback_comment(category, sentiment)
        
        try:
            # Rastgele kategori ve sentiment seçimi (bazen farklı kombinasyonlar)
            if random.random() < 0.1:  # %10 ihtimalle farklı kategori
                category = random.choice(self.categories)
            if random.random() < 0.1:  # %10 ihtimalle farklı sentiment
                sentiment = random.choice(self.sentiments)
            
            prompt = self.generate_comment_prompt(category, sentiment)
            
            # Temperature ayarı ile daha çeşitli yanıtlar
            response = await asyncio.to_thread(
                self.model.generate_content, 
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.8,  # Daha yüksek temperature = daha çeşitli
                    top_p=0.9,
                    top_k=40
                )
            )
            
            comment = response.text.strip()
            logger.info(f"Generated comment ({category}/{sentiment}): {comment[:50]}...")
            return comment
            
        except Exception as e:
            logger.error(f"Error generating comment: {e}")
            return self._get_fallback_comment(category, sentiment)
    
    def _get_fallback_comment(self, category: str, sentiment: str) -> str:
        """Fallback yorumlar"""
        fallback_comments = {
            "menu_variety": {
                "positive": [
                    "Menüde çok fazla seçenek var, herkes için bir şey bulunur.",
                    "Yemek çeşitliliği gerçekten etkileyici, her zevke uygun.",
                    "Menü çok zengin, tekrar gelmek için birçok neden var."
                ],
                "negative": [
                    "Menüde çok az seçenek var, sıkıcı.",
                    "Yemek çeşitliliği yetersiz, aynı şeyler tekrar ediyor.",
                    "Menü çok sınırlı, farklı bir şey bulamıyorum."
                ],
                "neutral": [
                    "Menüde standart seçenekler var, beklentileri karşılıyor.",
                    "Yemek çeşitliliği orta seviyede, yeterli.",
                    "Menü normal, ne fazla ne eksik."
                ]
            },
            "taste_quality": {
                "positive": [
                    "Yemekler çok lezzetliydi, tadı damağımda kaldı.",
                    "Lezzet kalitesi gerçekten yüksek, çok beğendim.",
                    "Yemeklerin tadı muhteşem, kesinlikle tekrar geleceğim."
                ],
                "negative": [
                    "Yemeklerin tadı beklentilerimi karşılamadı.",
                    "Lezzet kalitesi düşük, yemekler tatsız.",
                    "Yemeklerin tadı kötü, pişman oldum."
                ],
                "neutral": [
                    "Yemeklerin tadı normal, orta seviyede.",
                    "Lezzet kalitesi makul, beklentileri karşılıyor.",
                    "Yemeklerin tadı iyi, standart kalitede."
                ]
            },
            "service_speed": {
                "positive": [
                    "Servis çok hızlıydı, beklemeden yemeklerimiz geldi.",
                    "Personel çok hızlı ve verimli, teşekkürler.",
                    "Servis hızı mükemmel, çok memnun kaldım."
                ],
                "negative": [
                    "Servis çok yavaştı, yemeklerimiz geç geldi.",
                    "Personel yavaş, beklemek zorunda kaldık.",
                    "Servis hızı düşük, sabırsızlandım."
                ],
                "neutral": [
                    "Servis normal hızda, makul bir süre bekledik.",
                    "Personel orta hızda, standart servis.",
                    "Servis hızı normal, beklentileri karşılıyor."
                ]
            },
            "ambiance": {
                "positive": [
                    "Ortam çok güzeldi, romantik bir akşam yemeği için ideal.",
                    "Ambiyans harika, çok rahat ve keyifli bir atmosfer.",
                    "Ortam çok temiz ve düzenli, çok beğendim."
                ],
                "negative": [
                    "Ortam gürültülü ve rahatsız, keyif alamadım.",
                    "Ambiyans kötü, temizlik konusunda sorunlar var.",
                    "Ortam kalabalık ve sıkışık, rahat edemedim."
                ],
                "neutral": [
                    "Ortam normal, standart bir restoran atmosferi.",
                    "Ambiyans orta seviyede, yeterli.",
                    "Ortam makul, beklentileri karşılıyor."
                ]
            },
            "price_performance": {
                "positive": [
                    "Fiyatlar çok uygun, kaliteye göre çok iyi.",
                    "Fiyat/performans oranı mükemmel, çok değerli.",
                    "Kaliteye göre fiyatlar çok makul, memnun kaldım."
                ],
                "negative": [
                    "Fiyatlar çok yüksek, kaliteye değmez.",
                    "Fiyat/performans oranı kötü, pahalı.",
                    "Kaliteye göre fiyatlar yüksek, memnun değilim."
                ],
                "neutral": [
                    "Fiyatlar normal, kaliteye uygun.",
                    "Fiyat/performans oranı makul, standart.",
                    "Kaliteye göre fiyatlar orta seviyede."
                ]
            }
        }
        
        category_comments = fallback_comments.get(category, fallback_comments["menu_variety"])
        sentiment_comments = category_comments.get(sentiment, category_comments["neutral"])
        
        return random.choice(sentiment_comments)
    
    def create_comment_message(self, comment: str, category: str, sentiment: str) -> Dict[str, Any]:
        """Kafka mesajı oluştur"""
        comment_id = f"comment_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
        
        message = {
            "commentId": comment_id,
            "text": comment,
            "category": category,
            "sentiment": sentiment,
            "timestamp": datetime.now().isoformat(),
            "language": "tr",
            "source": "llm_generator"
        }
        
        return message
    
    def send_to_kafka(self, message: Dict[str, Any]) -> bool:
        """Kafka'ya mesaj gönder"""
        try:
            if not self.kafka_producer:
                self.connect_kafka()
            
            future = self.kafka_producer.send('raw-comments', message)
            record_metadata = future.get(timeout=10)
            
            logger.info(f"Comment sent to Kafka: {message['commentId']}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send comment to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending comment: {e}")
            return False
    
    async def generate_and_send_comment(self):
        """Yorum üret ve Kafka'ya gönder"""
        try:
            # Rastgele kategori ve sentiment seç
            category = random.choice(self.categories)
            sentiment = random.choice(self.sentiments)
            
            # Tekrar kontrolü
            if random.random() < self.repeat_probability and self.repeated_comments:
                # Tekrar eden yorum gönder
                repeated_comment = random.choice(self.repeated_comments)
                message = self.create_comment_message(
                    repeated_comment['text'],
                    repeated_comment['category'],
                    repeated_comment['sentiment']
                )
                logger.info("Sending repeated comment")
            else:
                # Yeni yorum üret
                comment = await self.generate_comment(category, sentiment)
                message = self.create_comment_message(comment, category, sentiment)
                
                # Tekrar için sakla
                if len(self.repeated_comments) < 10:
                    self.repeated_comments.append({
                        'text': comment,
                        'category': category,
                        'sentiment': sentiment
                    })
            
            # Kafka'ya gönder
            success = self.send_to_kafka(message)
            return success
            
        except Exception as e:
            logger.error(f"Error in generate_and_send_comment: {e}")
            return False
    
    async def run_continuous_generation(self):
        """Sürekli yorum üretimi"""
        logger.info("Starting continuous comment generation...")
        
        while True:
            try:
                # Rastgele aralık (2-5 saniye) - API limitlerini aşmamak için
                interval = random.uniform(0.01, 0.1)
                
                # Yorum üret ve gönder
                success = await self.generate_and_send_comment()
                
                if success:
                    logger.info(f"Comment generated and sent successfully. Next in {interval:.2f}s")
                else:
                    logger.warning("Failed to generate/send comment")
                
                # Bekle
                await asyncio.sleep(interval)
                
            except KeyboardInterrupt:
                logger.info("Comment generation stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in continuous generation: {e}")
                await asyncio.sleep(1)  # Hata durumunda 1 saniye bekle
    
    def close(self):
        """Kafka producer'ı kapat"""
        if self.kafka_producer:
            self.kafka_producer.close()
            logger.info("Kafka producer closed")

async def main():
    """Ana fonksiyon"""
    generator = CommentGenerator()
    
    try:
        await generator.run_continuous_generation()
    finally:
        generator.close()

if __name__ == "__main__":
    asyncio.run(main()) 
