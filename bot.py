# bot.py
# Full Club Auction Bot (Final Production Build - 100% Patched)
# Dependencies: discord.py, fastapi, uvicorn, jinja2, pymongo, dnspython, certifi

import os
import asyncio
import random
from datetime import datetime
import discord
from discord.ext import commands
from discord.ui import View, Button
from pymongo import MongoClient, ReturnDocument
import certifi

# ---------- CONFIGURATION ----------
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
MONGO_URL = os.getenv("MONGO_URL")

BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID")) if os.getenv("BOT_OWNER_ID") else None

# Log Channels
LOG_CHANNELS = {
    "withdraw": 1443955732281167873, # Payment/Payout Logs
    "battle": 1439844034905374720,
    "club": 1443955856222851142,
    "duelist": 1443955967086690395
}

# Constants
TIME_LIMIT = 90  # 1 Minute 30 Seconds
MIN_INCREMENT_PERCENT = 5
LEAVE_PENALTY_PERCENT = 10
DUELIST_MISS_PENALTY_PERCENT = 15
WIN_VALUE_BONUS = 100000
LOSS_VALUE_PENALTY = -100000
OWNER_MSG_VALUE_BONUS = 10000
OWNER_MSG_COUNT_PER_BONUS = 100

LEVEL_UP_CONFIG = [
    (12, "5th Division", 50000), (27, "4th Division", 100000), (45, "3rd Division", 150000),
    (66, "2nd Division", 200000), (90, "1st Division", 300000), (117, "17th Position", 320000),
    (147, "15th Position", 360000), (180, "12th Position", 400000), (216, "10th Position", 450000),
    (255, "8th Position", 500000), (297, "6th Position", 550000), (342, "Conference League", 600000),
    (390, "5th Position", 650000), (441, "Europa League", 700000), (495, "4th Position", 750000),
    (552, "3rd Position", 800000), (612, "Champions League", 900000), (675, "2nd Position", 950000),
    (741, "1st Position and League Winner", 1000000), (810, "UCL Winner", 1500000), (882, "Treble Winner", 2000000),
]

# Banter Lines
BATTLE_BANTER = [
    "🔥 Absolute demolition! Team A tore Team B apart like homework due tomorrow. What a massacre!",
    "🔥 Team A owned the pitch today — sent Team B home with a souvenir bag full of tears.",
    "🔥 That wasn’t a match… that was a public execution. RIP Team B.",
    "🔥 Team A delivered a masterclass — Team B just attended the lecture.",
    "🏟️ Fortress defended! Team A protected their home like it was the national treasury.",
    "🏟️ Welcome to our home — shoes off, ego off, points taken. Try again next time Team B.",
    "🚍 Silence in the stadium — Team A just robbed Team B in their own house.",
    "🚍 Fans left early, stadium empty — Team A evacuated Team B’s hopes.",
    "😈 From hopeless to ruthless — Team A just turned the match into a movie!",
    "😈 Comeback kings! Team A woke up and chose violence.",
    "💥 Team B tried their best, but sometimes best is not enough.",
    "💥 Painful defeat — that last moment hit harder than a tax bill.",
    "😭 Team B bottled it like a soda factory.",
    "😭 They had the lead… and then they donated it.",
]

# Emojis
E_ACTIVE = "<a:geeen_dot:1443252917648752681>"
E_DANGER = "<a:red_dot:1443261605092786188>"
E_ALERT = "<a:alert:1443254143308533863>" 
E_ERROR = "<a:cross2:972155180185452544>"
E_SUCCESS = "<a:verified:962942818886770688>"
E_GOLD_TICK = "<a:goldcheckmark:1443253229398917252>"
E_CROWN = "<a:crownop:962190451744579605>"
E_ADMIN = "<a:HeadAdmin_red:1443253359095058533>"
E_PREMIUM = "<a:donate_red:1443252440634884117>"
E_BOOST = "<a:boost:962277213204525086>"
E_PIKACHU = "<a:miapikachu:1443253477533814865>"
E_MONEY = "<a:Donation:962944611792326697>"
E_TIMER = "<a:1031pixelclock:1443253900793741332>"
E_STARS = "<a:bluestars:1443254349869486140>"
E_ITEMBOX = "<a:itembox:1443254784898367629>" 
E_FIRE = "<a:redfire1:1443251827490684938>"
E_ARROW = "<a:arrow_arrow:962945777821450270>"
E_RED_ARROW = "<a:redarrow:1443251741905653811>"
E_STAR = "<a:yellowstar:1443252221645950996>"
E_AUCTION = "<:Auction:1443250889266565190>"
E_BOOK = "<a:rules:1443252031220613321>"
E_NYAN = "<a:NyanCat:1443253686771126454>"
E_GIVEAWAY = "<a:gw:1443251079705001984>" 
E_CHAT = "<a:text:1443251311939293267>"

# Battle Specific Emojis
E_WINNER_TROPHY = ":918267trophy:"
E_LOSER_MARK = "<:loser:1443996248871928090>"

WINNER_REACTIONS = [
    ":7833dakorcalmao:", ":33730ohoholaugh:", ":44158laughs:", ":69692pepewine:", 
    ":954110babythink:", 1443996271990935552, 1443996261941383178, 
    1443996171071914177, 1443996142382612662, 1443996193012187217, 
    1443996181733834803, 1443996134031753306, 1443996236356259861, 
    ":10436batmanlaugh:", 1443996221608824922
]

LOSER_REACTIONS = [
    "<:192978sadchinareact:1443996152772038678>", "<:26955despair:1443996205028999360>", 
    "<:8985worldcup:1443996229620203712>", 1443996214805790871, 
    1443996269113643028, 1443996159080403174, 1443996188951973999, 
    1443996139362844783, 1443996248871928090
]

# ---------- DATABASE CONNECTION ----------
if not MONGO_URL:
    print("CRITICAL: MONGO_URL missing.")
    cluster = None
    db = None
else:
    try:
        cluster = MongoClient(MONGO_URL, tlsCAFile=certifi.where())
        db = cluster["auction_bot"]
        print("✅ MongoDB Connected.")
    except Exception as e:
        print(f"❌ MongoDB Connection Error: {e}")
        db = None

# Initialize Collections
if db is not None:
    clubs_col = db.clubs
    duelists_col = db.duelists
    groups_col = db.investor_groups
    group_members_col = db.groups_members
    wallets_col = db.personal_wallets
    profiles_col = db.user_profiles
    bids_col = db.bids
    history_col = db.club_history
    contracts_col = db.duelist_contracts
    battles_col = db.battle_register
    audit_col = db.audit_logs
    config_col = db.bot_config
    counters_col = db.counters
    past_entities_col = db.past_entities
    activities_col = db.user_activities

def get_next_id(sequence_name):
    if db is None: return 0
    ret = counters_col.find_one_and_update(
        {"_id": sequence_name}, {"$inc": {"seq": 1}},
        upsert=True, return_document=ReturnDocument.AFTER
    )
    return ret['seq']

# ---------- BOT SETUP ----------
DEFAULT_PREFIX = "."

def get_prefix(bot, message):
    if db is None: return DEFAULT_PREFIX
    try:
        res = config_col.find_one({"key": "prefix"})
        return res["value"] if res else DEFAULT_PREFIX
    except:
        return DEFAULT_PREFIX

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)
active_timers = {}
bidding_frozen = False

# ---------- HELPER CLASSES & FUNCTIONS ----------

class Paginator(View):
    def __init__(self, ctx, data, title, color, per_page=10):
        super().__init__(timeout=60)
        self.ctx = ctx
        self.data = data
        self.title = title
        self.color = color
        self.per_page = per_page
        self.current_page = 0
        self.total_pages = max(1, (len(data) + per_page - 1) // per_page)
        self.update_buttons()

    def update_buttons(self):
        self.children[0].disabled = self.current_page == 0
        self.children[1].disabled = self.current_page == self.total_pages - 1

    def get_embed(self):
        start = self.current_page * self.per_page
        end = start + self.per_page
        page_data = self.data[start:end]
        
        embed = discord.Embed(title=f"{self.title} ({self.current_page + 1}/{self.total_pages})", color=self.color)
        for name, value in page_data:
            embed.add_field(name=name, value=value, inline=False)
        return embed

    @discord.ui.button(emoji="⬅️", style=discord.ButtonStyle.primary)
    async def prev_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.ctx.author.id: return await interaction.response.send_message("Not your menu.", ephemeral=True)
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(emoji="➡️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.ctx.author.id: return await interaction.response.send_message("Not your menu.", ephemeral=True)
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

def create_embed(title, description, color=0x2ecc71, thumbnail=None, footer=None):
    embed = discord.Embed(title=title, description=description, color=color)
    if thumbnail and isinstance(thumbnail, str) and (thumbnail.startswith("http://") or thumbnail.startswith("https://")):
        embed.set_thumbnail(url=thumbnail)
    if footer:
        embed.set_footer(text=footer)
    return embed

def resolve_emoji(item):
    """Helper to convert ID integers to Discord Emoji Objects"""
    if isinstance(item, int) or (isinstance(item, str) and item.isdigit()):
        e = bot.get_emoji(int(item))
        return str(e) if e else "❓"
    return item

def log_audit(entry: str):
    if db is not None: audit_col.insert_one({"entry": entry, "timestamp": datetime.now()})

def log_user_activity(user_id, type, description):
    if db is not None:
        activities_col.insert_one({
            "user_id": str(user_id),
            "type": type,
            "description": description,
            "timestamp": datetime.now()
        })

async def send_log(channel_key, embed):
    if db is None: return
    cid = LOG_CHANNELS.get(channel_key)
    if cid:
        ch = bot.get_channel(cid)
        if ch: await ch.send(embed=embed)

def get_current_bid(item_type=None, item_id=None):
    if db is None: return 0
    if item_type and item_id is not None:
        bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
        if bid: return bid["amount"]
    
    if item_type == "club":
        c = clubs_col.find_one({"id": int(item_id)})
        return c["base_price"] if c else 0
    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        return d["base_price"] if d else 0
    return 0

def min_required_bid(current):
    add = current * MIN_INCREMENT_PERCENT / 100
    return int(current + max(1, round(add)))

def get_club_owner_info(club_id):
    if db is None: return None, []
    c = clubs_col.find_one({"id": int(club_id)})
    if not c or "owner_id" not in c or not c["owner_id"]:
        return None, []
    
    owner_str = c["owner_id"]
    if owner_str.startswith('group:'):
        gname = owner_str.replace('group:', '').lower()
        members = group_members_col.find({"group_name": gname})
        return owner_str, [m['user_id'] for m in members]
    return owner_str, [owner_str]

def get_level_info(current_wins, level_name=None):
    current_level = level_name if level_name else LEVEL_UP_CONFIG[0][1]
    next_level_info = None
    required_wins = 0
    for wins_required, name, bonus in LEVEL_UP_CONFIG:
        if wins_required > current_wins:
            next_level_info = (name, wins_required, bonus)
            required_wins = wins_required - current_wins
            break
        elif wins_required <= current_wins:
            current_level = name
    return current_level, next_level_info, required_wins

def update_club_level(club_id, wins_gained=0):
    if db is None: return
    c = clubs_col.find_one({"id": int(club_id)})
    if not c: return None
    new_wins = c.get("total_wins", 0) + wins_gained
    clubs_col.update_one({"id": int(club_id)}, {"$set": {"total_wins": new_wins}})
    for wins_required, name, bonus in LEVEL_UP_CONFIG:
        if c.get("total_wins", 0) < wins_required <= new_wins:
            clubs_col.update_one({"id": int(club_id)}, {"$set": {"level_name": name}, "$inc": {"value": bonus}})
            log_audit(f"Club {c['name']} leveled up to {name}. Bonus: {bonus}")
            return name
    return None

def log_past_entity(user_id, type, name):
    past_entities_col.insert_one({
        "user_id": str(user_id),
        "type": type, 
        "name": name,
        "timestamp": datetime.now()
    })

# ---------- BACKGROUND TASKS ----------
async def market_simulation_task():
    while True:
        await asyncio.sleep(3600)
        if db is not None:
            for c in clubs_col.find():
                base = c.get("value", c["base_price"])
                change = random.uniform(-0.03, 0.03)
                new_value = int(max(100, base * (1 + change)))
                clubs_col.update_one({"_id": c["_id"]}, {"$set": {"value": new_value}})

async def weekly_report_scheduler():
    while True:
        await asyncio.sleep(7 * 24 * 3600)
        pass

# ---------- AUCTION LOGIC ----------
async def finalize_auction(item_type: str, item_id: int, channel_id: int):
    if db is None: return
    winner_bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
    channel = bot.get_channel(channel_id)
    club_item = clubs_col.find_one({"id": int(item_id)}) if item_type == "club" else None
    
    if winner_bid:
        bidder_str = winner_bid["bidder"]
        amount = int(winner_bid["amount"])
        
        # Deduct from Bidder
        if bidder_str.startswith('group:'):
            gname = bidder_str.replace('group:', '').lower()
            groups_col.update_one({"name": gname}, {"$inc": {"funds": -amount}})
        else:
            wallets_col.update_one({"user_id": bidder_str}, {"$inc": {"balance": -amount}})
            log_user_activity(bidder_str, "Transaction", f"Paid ${amount:,} for Auction {item_type} {item_id}")
            
        if item_type == "club":
            # Handle Club Sale
            old_owner = club_item.get("owner_id")
            if old_owner and not old_owner.startswith("group:"):
                profiles_col.update_one({"user_id": old_owner}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
                log_past_entity(old_owner, "ex_owner", club_item["name"])

            history_col.insert_one({"club_id": int(item_id), "winner": bidder_str, "amount": amount, "timestamp": datetime.now(), "market_value_at_sale": club_item.get("value", 0)})
            
            # UPDATE: Market Value = Last Bid
            clubs_col.update_one({"id": int(item_id)}, {
                "$set": {
                    "owner_id": bidder_str, 
                    "last_bid_price": amount,
                    "value": amount, 
                    "ex_owner_id": old_owner
                }
            })
            
            if not bidder_str.startswith('group:'):
                profiles_col.update_one({"user_id": bidder_str}, {"$set": {"owned_club_id": int(item_id), "owned_club_share": 100}}, upsert=True)
                log_user_activity(bidder_str, "Win", f"Won Auction for Club {club_item['name']}")
            
            if channel:
                await channel.send(embed=create_embed(
                    f"{E_GIVEAWAY} AUCTION SOLD & CONFIRMED", 
                    f"The gavel has fallen! Ownership has been transferred.\n\n"
                    f"{E_SUCCESS} **New Owner:** {bidder_str}\n"
                    f"{E_ITEMBOX} **Club:** {club_item['name']}\n"
                    f"{E_MONEY} **Final Price:** ${amount:,}\n"
                    f"{E_STARS} **New Market Value:** ${amount:,}", 
                    color=0xf1c40f, 
                    thumbnail=club_item.get("logo")
                ))
                
        else: 
            # Handle Duelist Sale
            d_item = duelists_col.find_one({"id": int(item_id)})
            salary = d_item["expected_salary"]
            contracts_col.insert_one({"duelist_id": int(item_id), "club_owner": bidder_str, "purchase_price": amount, "salary": salary, "signed_at": datetime.now()})
            
            target_club_id = None
            if bidder_str.startswith('group:'):
                gname = bidder_str.replace('group:', '').lower()
                c = clubs_col.find_one({"owner_id": f"group:{gname}"})
                if c: target_club_id = c['id']
            else:
                c = clubs_col.find_one({"owner_id": bidder_str})
                if c: target_club_id = c['id']
            
            duelists_col.update_one({"id": int(item_id)}, {"$set": {"owned_by": bidder_str, "club_id": target_club_id}})

            # Credit Duelist Wallet
            wallets_col.update_one(
                {"user_id": d_item["discord_user_id"]}, 
                {"$inc": {"balance": amount}}, 
                upsert=True
            )
            log_user_activity(d_item["discord_user_id"], "Transaction", f"Received ${amount:,} Signing Fee.")
            
            # FIX: Use E_ITEMBOX
            if channel:
                 await channel.send(embed=create_embed(
                    f"{E_GIVEAWAY} DUELIST SIGNED & CONFIRMED", 
                    f"Contract signed! Transfer complete.\n\n"
                    f"{E_SUCCESS} **Signed To:** {bidder_str}\n"
                    f"{E_ITEMBOX} **Player:** {d_item['username']}\n"
                    f"{E_MONEY} **Transfer Fee:** ${amount:,} (Paid to Player)", 
                    color=0x9b59b6,
                    thumbnail=d_item.get('avatar_url')
                ))
    else:
        if channel: await channel.send(embed=create_embed(f"{E_TIMER} Auction Ended", "No bids were placed.", color=0x95a5a6))
    
    bids_col.delete_many({"item_type": item_type, "item_id": int(item_id)})
    active_timers.pop((item_type, str(item_id)), None)

def schedule_auction_timer(item_type: str, item_id: int, channel_id: int):
    key = (item_type, str(item_id))
    if active_timers.get(key) and not active_timers.get(key).done(): active_timers[key].cancel()
    loop = asyncio.get_event_loop()
    t = loop.create_task(asyncio.sleep(TIME_LIMIT))
    async def wrapper():
        try: await t; await finalize_auction(item_type, item_id, channel_id)
        except asyncio.CancelledError: return
    active_timers[key] = loop.create_task(wrapper())

# ---------- EVENTS ----------
@bot.event
async def on_command_completion(ctx):
    log_user_activity(ctx.author.id, "Command", f"Used {E_CHAT} `.{ctx.command.name}`")

# ===========================
#  GROUP 1: USER & ECONOMY
# ===========================

@bot.hybrid_command(name="playerhistory", description="Admin: View full user history.")
@commands.has_permissions(administrator=True)
async def playerhistory(ctx, user: discord.Member):
    uid = str(user.id)
    w = wallets_col.find_one({"user_id": uid})
    bal = w["balance"] if w else 0
    past_clubs = list(past_entities_col.find({"user_id": uid, "type": "ex_owner"}))
    past_groups = list(past_entities_col.find({"user_id": uid, "type": "ex_member"}))
    acts = list(activities_col.find({"user_id": uid}).sort("timestamp", -1).limit(50))
    
    data = []
    summary = f"**Wallet:** ${bal:,}\n**Ex-Clubs:** {', '.join([p['name'] for p in past_clubs]) or 'None'}\n**Ex-Groups:** {', '.join([p['name'].title() for p in past_groups]) or 'None'}"
    data.append((f"{E_CROWN} User Summary", summary))
    for act in acts:
        ts = act['timestamp'].strftime("%Y-%m-%d %H:%M")
        icon = E_CHAT if act['type'] == "Command" else E_MONEY
        data.append((f"{icon} {act['type']} - {ts}", f"{act['description']}"))
    if not data: data.append(("No History", "This user has no recorded history."))
    view = Paginator(ctx, data, f"{E_BOOK} History: {user.display_name}", 0x3498db, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="profile", description="View your or another user's profile stats.")
async def profile(ctx, member: discord.Member = None):
    member = member or ctx.author
    uid = str(member.id)
    w = wallets_col.find_one({"user_id": uid})
    bal = w["balance"] if w else 0
    
    thumbnail_url = member.avatar.url if member.avatar else None
    group_mem = group_members_col.find_one({"user_id": uid})
    if group_mem:
        g_info = groups_col.find_one({"name": group_mem['group_name']})
        if g_info and g_info.get('logo'):
            thumbnail_url = g_info['logo']

    embed = create_embed(f"{E_CROWN} Profile", f"**User:** {member.mention}\n{E_MONEY} **Balance:** ${bal:,}", 0x3498db, thumbnail=thumbnail_url)
    
    groups = list(group_members_col.find({"user_id": uid}))
    g_list = [f"{g['group_name'].title()} ({g['share_percentage']}%)" for g in groups]
    embed.add_field(name="Groups", value=", ".join(g_list) if g_list else "None", inline=False)
    
    # Past Groups
    past_groups = list(past_entities_col.find({"user_id": uid, "type": "ex_member"}))
    pg_list = [p['name'].title() for p in past_groups]
    if pg_list: embed.add_field(name="Ex-Group Member", value=", ".join(pg_list), inline=False)
    
    bids = list(bids_col.find({"bidder": uid}).sort("timestamp", -1).limit(5))
    b_list = [f"{b['item_type'].title()} ID {b['item_id']}: ${b['amount']:,}" for b in bids]
    embed.add_field(name="Recent Bids", value="\n".join(b_list) if b_list else "None", inline=False)
    
    prof = profiles_col.find_one({"user_id": uid})
    if prof and prof.get("owned_club_id"):
        c = clubs_col.find_one({"id": prof["owned_club_id"]})
        if c: embed.add_field(name="Owned Club", value=f"{c['name']} (100%)", inline=False)
    
    # Past Clubs
    past_clubs = list(past_entities_col.find({"user_id": uid, "type": "ex_owner"}))
    pc_list = [p['name'] for p in past_clubs]
    if pc_list: embed.add_field(name="Ex-Owner", value=", ".join(pc_list), inline=False)

    duelist = duelists_col.find_one({"discord_user_id": uid})
    if duelist:
        cname = "Free Agent"
        if duelist.get("club_id"):
            c = clubs_col.find_one({"id": duelist["club_id"]})
            if c: cname = c["name"]
        embed.add_field(name="Duelist Status", value=f"Club: {cname}\nSalary: ${duelist['expected_salary']:,}", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="wallet", description="Check your current personal balance.")
async def wallet(ctx):
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    bal = w["balance"] if w else 0
    embed = create_embed(
        f"{E_MONEY} {E_NYAN} Wallet Balance",
        f"**User:** {ctx.author.mention}\n**Cash:** ${bal:,}",
        0x2ecc71, thumbnail=ctx.author.avatar.url if ctx.author.avatar else None
    )
    await ctx.send(embed=embed)

@bot.hybrid_command(name="payout", description="Admin: Pay a user from the system (deducts their wallet balance).")
@commands.has_permissions(administrator=True)
async def payout(ctx, user: discord.Member, amount: int, *, reason: str):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    
    w = wallets_col.find_one({"user_id": str(user.id)})
    if not w or w.get("balance", 0) < amount:
         return await ctx.send(embed=create_embed("Error", f"{E_ERROR} User has insufficient funds to cash out.", 0xff0000))
    
    wallets_col.update_one({"user_id": str(user.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(user.id, "Payout", f"Cashed out ${amount:,} by {ctx.author.name}. Reason: {reason}")
    
    embed_log = create_embed(
        f"{E_MONEY} Payout Log",
        f"**Paid To:** {user.mention}\n**Paid By:** {ctx.author.mention}\n**Amount:** ${amount:,}\n**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n**Reason:** {reason}",
        0xe74c3c
    )
    await send_log("withdraw", embed_log)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Payout Successful", f"Processed payout of **${amount:,}** for {user.mention}.", 0x2ecc71))

@bot.hybrid_command(name="withdrawwallet", description="Burn/Delete money from your wallet.")
async def withdrawwallet(ctx, amount: int):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Burned ${amount:,} from wallet.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdrawn", f"Removed **${amount:,}** from wallet.", 0x2ecc71))

@bot.hybrid_command(name="grouplist", description="List all investor groups.")
async def grouplist(ctx):
    groups = list(groups_col.find())
    data = []
    for g in groups:
        data.append((g['name'].title(), f"{E_MONEY} ${g['funds']:,}"))
    view = Paginator(ctx, data, f"{E_PREMIUM} Group List", 0x9b59b6, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="groupinfo", description="Get detailed info about a group.")
async def groupinfo(ctx, *, group_name: str):
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Group not found.", 0xff0000))
    
    members = list(group_members_col.find({"group_name": gname}))
    clubs = list(clubs_col.find({"owner_id": f"group:{gname}"}))
    
    embed = discord.Embed(title=f"{E_PREMIUM} Group: {g['name'].title()}", color=0x9b59b6)
    if g.get('logo'): embed.set_thumbnail(url=g['logo'])
         
    embed.add_field(name="Bank", value=f"{E_MONEY} ${g['funds']:,}", inline=True)
    
    # SAFE TRUNCATION
    member_display = members[:15] 
    mlist = []
    for m in member_display:
        try: u = await bot.fetch_user(int(m['user_id'])); name = u.name
        except: name = "Unknown"
        mlist.append(f"{E_ARROW} {name}: {m['share_percentage']}%")
    if len(members) > 15: mlist.append(f"...and {len(members)-15} more.")
    embed.add_field(name=f"Members ({len(members)})", value="\n".join(mlist) or "None", inline=False)
    
    ex_members = list(past_entities_col.find({"type": "ex_member", "name": gname}))
    ex_list = [ex['user_id'] for ex in ex_members]
    if ex_list: embed.add_field(name="Ex-Members", value=", ".join(ex_list[:5]), inline=False)
    
    clist = [c['name'] for c in clubs]
    embed.add_field(name="Clubs Owned", value=", ".join(clist) or "None", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="creategroup", description="Create a new investment group (attach logo image).")
async def creategroup(ctx, name: str, share: int):
    gname = name.lower()
    if groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group exists.", 0xff0000))
    
    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    groups_col.insert_one({"name": gname, "funds": 0, "owner_id": str(ctx.author.id), "logo": logo_url})
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Created group {name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Created", f"Group **{name}** created with **{share}%** share.\nLogo Set: {'Yes' if logo_url else 'No'}", 0x2ecc71, thumbnail=logo_url))

@bot.hybrid_command(name="joingroup", description="Join an existing group.")
async def joingroup(ctx, name: str, share: int):
    gname = name.lower()
    if not groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}):
        return await ctx.send(embed=create_embed("Error", "Already a member.", 0xff0000))
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Joined group {name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Joined", f"Joined **{name}** with **{share}%**.", 0x2ecc71))

@bot.hybrid_command(name="deposit", description="Transfer personal funds to group bank.")
async def deposit(ctx, group_name: str, amount: int):
    if amount <= 0: return
    gname = group_name.lower()
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}):
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Not a member.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount:
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    groups_col.update_one({"name": gname}, {"$inc": {"funds": amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Deposited ${amount:,} to {group_name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deposit", f"Deposited **${amount:,}** to **{group_name}**.", 0x2ecc71))

@bot.hybrid_command(name="withdraw", description="Withdraw funds from group bank to wallet.")
async def withdraw(ctx, group_name: str, amount: int):
    gname = group_name.lower()
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    g = groups_col.find_one({"name": gname})
    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -amount}})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Withdrew ${amount:,} from {group_name}.")
    
    # Log is purely internal unless configured otherwise
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdraw", f"Withdrew **${amount:,}**.", 0x2ecc71))

@bot.hybrid_command(name="leavegroup", description="Leave an investment group.")
async def leavegroup(ctx, name: str):
    gname = name.lower()
    mem = group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)})
    if not mem: return await ctx.send(embed=create_embed("Error", "Not a member.", 0xff0000))
    if mem['share_percentage'] > 0: return await ctx.send(embed=create_embed("Error", "Sell shares first.", 0xff0000))
    g = groups_col.find_one({"name": gname})
    penalty = int(g["funds"] * (LEAVE_PENALTY_PERCENT / 100))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -penalty}})
    group_members_col.delete_one({"_id": mem["_id"]})
    log_past_entity(ctx.author.id, "ex_member", gname)
    log_user_activity(ctx.author.id, "Group", f"Left group {name}.")
    await ctx.send(embed=create_embed(f"{E_DANGER} Left Group", f"Left **{name}**. Penalty: **${penalty:,}**.", 0xff0000))

@bot.hybrid_command(name="depositwallet", description="Restricted command.")
async def depositwallet(ctx, amount: int = None):
    await ctx.send(embed=create_embed(f"{E_DANGER} Restricted", "Use `.deposit <Group> <Amt>` to fund group.", 0xff0000))

# ===========================
#  GROUP 2: MARKET & BIDS
# ===========================

@bot.hybrid_command(name="placebid", description="Place a bid on a club or duelist.")
async def placebid(ctx, amount: int, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", f"{E_DANGER} Auctions frozen.", 0xff0000))
    item_type = item_type.lower()
    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        is_active = (item_type, str(item_id)) in active_timers
        if d.get("owned_by") and not is_active:
             return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This duelist is already signed.", 0xff0000))

        if not club_name: return await ctx.send(embed=create_embed("Error", "Provide club name.", 0xff0000))
        c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
        if not c:
            return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
        allowed = False
        if str(ctx.author.id) == c.get("owner_id"):
            allowed = True
        elif c.get("owner_id", "").startswith("group:"):
            gname = c.get("owner_id").replace("group:", "")
            if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}):
                allowed = True
        if not allowed: return await ctx.send(embed=create_embed("Error", "You/Group don't own this club.", 0xff0000))
    
    if item_type == "club":
         c = clubs_col.find_one({"id": int(item_id)})
         if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
         is_active = (item_type, str(item_id)) in active_timers
         
         # Logic: If owned AND no timer running = Sold Out
         if c.get("owner_id") and not is_active:
             return await ctx.send(embed=create_embed("Sold Out", f"{E_ERROR} This club is **SOLD OUT**. Wait for owner to sell.", 0xff0000))
         
         # Integrity: Check if user already owns a club
         prof = profiles_col.find_one({"user_id": str(ctx.author.id)})
         if prof and prof.get("owned_club_id"):
              return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You already own a club (100%). Sell it first.", 0xff0000))
    
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    # FIX: Handle None wallet gracefully
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    
    req = min_required_bid(get_current_bid(item_type, item_id))
    if amount < req: return await ctx.send(embed=create_embed("Bid Error", f"Min bid is ${req:,}", 0xff0000))
    
    bids_col.insert_one({"bidder": str(ctx.author.id), "amount": amount, "item_type": item_type, "item_id": int(item_id), "timestamp": datetime.now()})
    log_user_activity(ctx.author.id, "Bid", f"Placed bid of ${amount:,} on {item_type} {item_id}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Bid Placed", f"Bid of **${amount:,}** accepted.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="groupbid", description="Place a bid using group funds.")
async def groupbid(ctx, group_name: str, amount: int, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", "Auctions frozen.", 0xff0000))
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    
    if item_type == "club":
         c = clubs_col.find_one({"id": int(item_id)})
         is_active = (item_type, str(item_id)) in active_timers
         if c.get("owner_id") and not is_active:
              return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This club is **SOLD OUT**.", 0xff0000))
         
         # Group Integrity: One Club Per Group
         if clubs_col.find_one({"owner_id": f"group:{gname}"}):
              return await ctx.send(embed=create_embed("Error", "Group already owns a club.", 0xff0000))

    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        is_active = (item_type, str(item_id)) in active_timers
        if d.get("owned_by") and not is_active:
             return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This duelist is signed.", 0xff0000))

        if not club_name: return await ctx.send(embed=create_embed("Error", "Provide club name.", 0xff0000))
        c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
        if not c or c.get("owner_id") != f"group:{gname}": return await ctx.send(embed=create_embed("Error", "Group doesn't own club.", 0xff0000))
        
    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    
    bids_col.insert_one({"bidder": f"group:{gname}", "amount": amount, "item_type": item_type, "item_id": int(item_id)})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Bid", f"Group **{group_name}** bid **${amount:,}**.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="sellclub", description="Sell your club to another user or the market.")
async def sellclub(ctx, club_name: str, buyer: discord.Member = None):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    if str(ctx.author.id) != c.get("owner_id"): return await ctx.send(embed=create_embed("Error", "You don't own this.", 0xff0000))
    
    val = c["value"]
    target = buyer if buyer else "The Market"
    await ctx.send(embed=create_embed(f"{E_ALERT} Confirm Sale", f"Sell **{c['name']}** to {target.mention if buyer else 'Market'} for **${val:,}**?\nType `yes` or `no`.", 0xe67e22))
    
    def check(m): return m.author == (buyer if buyer else ctx.author) and m.content.lower() in ['yes', 'no']
    try: msg = await bot.wait_for('message', check=check, timeout=30.0)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    if msg.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled.", 0x95a5a6))
    
    # History
    old_owner = c.get("owner_id")
    if old_owner:
        profiles_col.update_one({"user_id": old_owner}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
        log_past_entity(old_owner, "ex_owner", c['name'])

    if buyer:
        bw = wallets_col.find_one({"user_id": str(buyer.id)})
        if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
        wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": str(buyer.id), "ex_owner_id": old_owner}})
        profiles_col.update_one({"user_id": str(buyer.id)}, {"$set": {"owned_club_id": c["id"], "owned_club_share": 100}}, upsert=True)
        profiles_col.update_one({"user_id": str(ctx.author.id)}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
        log_user_activity(buyer.id, "Transaction", f"Bought {c['name']} for ${val:,}")
    else:
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": None, "ex_owner_id": old_owner}})
        profiles_col.update_one({"user_id": str(ctx.author.id)}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
        
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
    embed_log = create_embed(f"{E_ADMIN} Club Sold", f"**Club:** {c['name']}\n**Seller:** {ctx.author.mention}\n**Buyer:** {target.mention if buyer else 'Market'}\n**Price:** ${val:,}", 0xe67e22)
    await send_log("club", embed_log)
    log_user_activity(ctx.author.id, "Sale", f"Sold club {c['name']} for ${val:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", f"Club sold for **${val:,}**.", 0x2ecc71))

@bot.hybrid_command(name="sellshares", description="Sell your group shares to another user.")
async def sellshares(ctx, club_name: str, buyer: discord.Member, percentage: int):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    owner_str = c.get("owner_id", "")
    if not owner_str.startswith("group:"): return await ctx.send(embed=create_embed("Error", "Not group owned.", 0xff0000))
    gname = owner_str.replace("group:", "")
    
    seller = group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)})
    if not seller or seller["share_percentage"] < percentage: return await ctx.send(embed=create_embed("Error", "Not enough shares.", 0xff0000))
    
    val = int(c["value"] * (percentage / 100))
    await ctx.send(embed=create_embed(f"{E_ALERT} Confirm Share Sale", f"{buyer.mention}, buy **{percentage}%** shares for **${val:,}**? `yes`/`no`", 0xe67e22))
    def check(m): return m.author == buyer and m.content.lower() in ['yes', 'no']
    try: msg = await bot.wait_for('message', check=check, timeout=30)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    
    if msg.content.lower() == 'yes':
        bw = wallets_col.find_one({"user_id": str(buyer.id)})
        if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
        
        wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
        
        group_members_col.update_one({"_id": seller["_id"]}, {"$inc": {"share_percentage": -percentage}})
        group_members_col.update_one({"group_name": gname, "user_id": str(buyer.id)}, {"$inc": {"share_percentage": percentage}}, upsert=True)
        log_user_activity(ctx.author.id, "Sale", f"Sold {percentage}% shares of {gname}.")
        log_user_activity(buyer.id, "Transaction", f"Bought {percentage}% shares of {gname}.")
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", "Shares transferred.", 0x2ecc71))

@bot.hybrid_command(name="listclubs", description="List all registered clubs.")
async def listclubs(ctx):
    clubs = list(clubs_col.find().sort("value", -1))
    data = []
    for c in clubs:
        data.append((f"{E_STAR} {c['name']} (ID: {c['id']})", f"{E_MONEY} ${c['value']:,} | {E_BOOST} {c.get('level_name')} | {E_FIRE} Wins: {c.get('total_wins',0)}"))
    view = Paginator(ctx, data, f"{E_CROWN} Registered Clubs", 0x3498db, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="listduelists", description="List all registered duelists.")
async def listduelists(ctx):
    ds = list(duelists_col.find())
    data = []
    for d in ds:
        cname = "Free Agent"
        if d.get("club_id"):
            c = clubs_col.find_one({"id": d["club_id"]})
            if c: cname = c["name"]
        # FIX: E_ITEMBOX used (variable defined)
        data.append((f"{d['username']}", f"{E_ITEMBOX} ID: {d['id']}\n{E_MONEY} ${d['expected_salary']:,}\n{E_STAR} {cname}"))
    view = Paginator(ctx, data, f"{E_BOOK} Duelist Registry", 0x9b59b6, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="clubinfo", description="Get detailed info about a club.")
async def clubinfo(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    
    owner_display = c.get('owner_id') or "Unowned"
    shareholder_text = ""
    if owner_display.startswith('group:'): 
        gname = owner_display.replace('group:', '').title()
        owner_display = f"Group: {gname}"
        # Logic to show shareholders
        raw_gname = c.get('owner_id').replace('group:', '').lower()
        members = list(group_members_col.find({"group_name": raw_gname}))
        shares = []
        for m in members:
             try: u = await bot.fetch_user(int(m['user_id'])); name = u.name
             except: name = "Unknown"
             if m['share_percentage'] > 0: shares.append(f"{name}: {m['share_percentage']}%")
        if shares: shareholder_text = "\n**Shareholders:**\n" + "\n".join(shares)

    else:
        try:
            if owner_display != "Unowned":
                owner_user = await bot.fetch_user(int(owner_display))
                owner_display = f"User: {owner_user.display_name}"
        except: pass
    
    ex_owner_display = "None"
    if c.get("ex_owner_id"):
        try: 
             ex_user = await bot.fetch_user(int(c["ex_owner_id"]))
             ex_owner_display = ex_user.display_name
        except: pass

    manager_name = "None"
    if c['manager_id']:
        try: u = await bot.fetch_user(int(c['manager_id'])); manager_name = u.name
        except: pass
    
    duelists = list(duelists_col.find({"club_id": c['id']}))
    d_list = "\n".join([f"{E_ARROW} {d['username']}" for d in duelists]) or "None"
    
    embed = discord.Embed(title=f"{E_CROWN} {c['name']}", description=f"{E_BOOST} **{c.get('level_name')}**{shareholder_text}", color=0x3498db)
    if c.get("logo") and isinstance(c["logo"], str) and (c["logo"].startswith("http://") or c["logo"].startswith("https://")):
        embed.set_thumbnail(url=c["logo"])
    embed.add_field(name="Owner", value=f"{E_STAR} {owner_display}", inline=True)
    embed.add_field(name="Ex-Owner", value=f"{E_RED_ARROW} {ex_owner_display}", inline=True)
    embed.add_field(name="Value", value=f"{E_MONEY} ${c['value']:,}", inline=True)
    embed.add_field(name="Manager", value=f"{E_ADMIN} {manager_name}", inline=True)
    embed.add_field(name=f"{E_ITEMBOX} Duelists", value=d_list, inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="clublevel", description="Check club level and division.")
async def clublevel(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    cur, nxt, req = get_level_info(c.get('total_wins', 0), c.get('level_name'))
    embed = create_embed(f"{E_BOOST} Club Level", f"**{c['name']}**\n{E_CROWN} Current: **{cur}**\n{E_FIRE} Wins: **{c.get('total_wins',0)}**", 0xf1c40f)
    if nxt: embed.add_field(name="Next Level", value=f"{E_ARROW} **{nxt[0]}**\n{E_RED_ARROW} Needs **{req}** wins")
    else: embed.add_field(name="Status", value=f"{E_GOLD_TICK} Max Level")
    await ctx.send(embed=embed)

@bot.hybrid_command(name="marketpanel", description="View the market status of a club.")
async def marketpanel(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    cur_lvl, nxt_lvl, req_wins = get_level_info(c.get('total_wins', 0), c.get('level_name'))
    embed = discord.Embed(title=f"{E_STARS} Market Panel: {c['name']}", color=0xf1c40f)
    if c.get("logo") and isinstance(c["logo"], str) and (c["logo"].startswith("http://") or c["logo"].startswith("https://")):
        embed.set_thumbnail(url=c["logo"])
    embed.add_field(name="Market Value", value=f"{E_MONEY} **${c['value']:,}**", inline=True)
    embed.add_field(name="Total Wins", value=f"{E_FIRE} **{c.get('total_wins', 0)}**", inline=True)
    embed.add_field(name="Division", value=f"{E_CROWN} **{cur_lvl}**", inline=False)
    if nxt_lvl: embed.add_field(name="Next", value=f"{E_ARROW} **{nxt_lvl[0]}** (Req: {req_wins} wins)", inline=True)
    else: embed.add_field(name="Status", value=f"{E_GOLD_TICK} Max Level", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="leaderboard", description="View top clubs.")
async def leaderboard(ctx):
    clubs = list(clubs_col.find().sort([("total_wins", -1), ("value", -1)]))
    data = []
    for i, c in enumerate(clubs):
        data.append((f"**{i+1}. {c['name']}**", f"{E_ARROW} {c.get('level_name')} | {E_FIRE} {c.get('total_wins')} Wins | {E_MONEY} ${c['value']:,}"))
    view = Paginator(ctx, data, f"{E_CROWN} Club Leaderboard", 0xf1c40f, 10)
    await ctx.send(embed=view.get_embed(), view=view)

# ===========================
#  GROUP 3: DUELIST & OWNERSHIP
# ===========================

@bot.hybrid_command(name="registerduelist", description="Register as a duelist.")
async def registerduelist(ctx, username: str, base_price: int, salary: int):
    if duelists_col.find_one({"discord_user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Already registered.", 0xff0000))
    did = get_next_id("duelist_id")
    avatar = ctx.author.avatar.url if ctx.author.avatar else ""
    duelists_col.insert_one({"id": did, "discord_user_id": str(ctx.author.id), "username": username, "base_price": base_price, "expected_salary": salary, "avatar_url": avatar, "owned_by": None, "club_id": None})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Registered", f"Duelist **{username}** (ID: {did})", 0x9b59b6))

@bot.hybrid_command(name="retireduelist", description="Retire a duelist.")
async def retireduelist(ctx, member: discord.Member = None):
    target_id = str(member.id) if member else str(ctx.author.id)
    d = duelists_col.find_one({"discord_user_id": target_id})
    if not d: return await ctx.send(embed=create_embed("Error", "Not a duelist.", 0xff0000))
    
    if member:
        if not d.get("club_id"): return await ctx.send(embed=create_embed("Error", "Free agent must self-retire.", 0xff0000))
        c = clubs_col.find_one({"id": d["club_id"]})
        owner_str, owner_ids = get_club_owner_info(c["id"])
        if str(ctx.author.id) not in owner_ids: return await ctx.send(embed=create_embed("Error", "Not owner.", 0xff0000))
        
        await ctx.send(embed=create_embed(f"{E_ALERT} Confirm", f"Owner {ctx.author.mention}, retire {member.mention}? `yes`/`no`", 0xe67e22))
        try: 
            msg = await bot.wait_for('message', check=lambda m: m.author==ctx.author and m.content.lower() in ['yes','no'], timeout=30)
            if msg.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled.", 0x95a5a6))
        except: return
        
        await ctx.send(embed=create_embed(f"{E_ALERT} Confirm", f"Duelist {member.mention}, confirm retirement? `yes`/`no`", 0xe67e22))
        try:
            msg2 = await bot.wait_for('message', check=lambda m: m.author==member and m.content.lower() in ['yes','no'], timeout=30)
            if msg2.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled by duelist.", 0x95a5a6))
        except: return
    else:
        if d.get("owned_by"): return await ctx.send(embed=create_embed("Error", "You are signed. Ask owner.", 0xff0000))
    duelists_col.delete_one({"_id": d["_id"]})
    embed_log = create_embed(f"{E_DANGER} Duelist Retired", f"**Player:** {d['username']}\n**ID:** {d['id']}", 0xff0000)
    await send_log("duelist", embed_log)
    log_user_activity(target_id, "Duelist", "Retired")
    await ctx.send(embed=create_embed(f"{E_DANGER} Retired", f"Duelist **{d['username']}** retired.", 0xff0000))

@bot.hybrid_command(name="adjustsalary", description="Owner: Pay bonus or fine duelist.")
async def adjustsalary(ctx, duelist_id: int, amount: int):
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d or not d.get("club_id"): return await ctx.send(embed=create_embed("Error", "Duelist not found/signed.", 0xff0000))
    c = clubs_col.find_one({"id": d["club_id"]})
    owner_str, owner_ids = get_club_owner_info(c["id"])
    if str(ctx.author.id) not in owner_ids: return await ctx.send(embed=create_embed("Error", "Not owner.", 0xff0000))
    
    if amount > 0:
        w = wallets_col.find_one({"user_id": str(ctx.author.id)})
        if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
        wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": amount}}, upsert=True)
        log_user_activity(ctx.author.id, "Transaction", f"Paid bonus ${amount:,} to {d['username']}")
        await ctx.send(embed=create_embed(f"{E_MONEY} Bonus", f"Paid **${amount:,}** to {d['username']}.", 0x2ecc71))
    else:
        abs_amt = abs(amount)
        wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": -abs_amt}}, upsert=True)
        # Return fine to owner
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": abs_amt}}, upsert=True)
        log_user_activity(ctx.author.id, "Transaction", f"Fined {d['username']} ${abs_amt:,}")
        await ctx.send(embed=create_embed(f"{E_DANGER} Fine", f"Deducted **${abs_amt:,}** from {d['username']}.", 0xff0000))

@bot.hybrid_command(name="deductsalary", description="Owner: Deduct salary for missed match.")
async def deductsalary(ctx, duelist_id: int, confirm: str):
    if confirm.lower() != "yes": return
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d: return await ctx.send(embed=create_embed("Error", "Duelist not found.", 0xff0000))
    
    # FIX: Validate Owner Logic
    if not d.get('club_id'): return await ctx.send(embed=create_embed("Error", "Duelist not in a club.", 0xff0000))
    owner_str, owner_ids = get_club_owner_info(d['club_id'])
    if str(ctx.author.id) not in owner_ids and not ctx.author.guild_permissions.administrator:
         return await ctx.send(embed=create_embed("Error", "Not authorized.", 0xff0000))

    penalty = int(d["expected_salary"] * (DUELIST_MISS_PENALTY_PERCENT / 100))
    # FIX: Deduct from Duelist
    wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": -penalty}}, upsert=True)
    # Add to Owner
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": penalty}}, upsert=True)
    log_user_activity(d["discord_user_id"], "Penalty", f"Fined ${penalty:,} for missed match.")
    await ctx.send(embed=create_embed(f"{E_ALERT} Penalty", f"Fined **${penalty:,}** from **{d['username']}**'s wallet.", 0xff0000))

# ===========================
#  GROUP 4: ADMIN & MOD
# ===========================

@bot.hybrid_command(name="registerclub", description="Admin: Register a new club (attach logo).")
@commands.has_permissions(administrator=True)
async def registerclub(ctx, name: str, base_price: int, *, slogan: str = ""):
    if clubs_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}}):
        return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club registered.", 0xff0000))
    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    cid = get_next_id("club_id")
    clubs_col.insert_one({"id": cid, "name": name, "base_price": base_price, "value": base_price, "slogan": slogan, "logo": logo_url, "total_wins": 0, "level_name": LEVEL_UP_CONFIG[0][1], "owner_id": None, "manager_id": None})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Club Registered", f"{E_ARROW} **Name:** {name}\n{E_MONEY} **Base:** ${base_price:,}\n{E_ITEMBOX} **ID:** {cid}", 0x2ecc71, thumbnail=logo_url))

@bot.hybrid_command(name="startclubauction", description="Admin: Start an auction for a club.")
@commands.has_permissions(administrator=True)
async def startclubauction(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    bids_col.delete_many({"item_type": "club", "item_id": c["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Auction Started", f"{E_ARROW} **Club:** {c['name']}\n{E_MONEY} **Base:** ${c['base_price']:,}", 0xe67e22, thumbnail=c.get('logo')))
    schedule_auction_timer("club", c["id"], ctx.channel.id)

@bot.hybrid_command(name="startduelistauction", description="Admin: Start an auction for a duelist.")
@commands.has_permissions(administrator=True)
async def startduelistauction(ctx, duelist_id: int):
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Duelist not found.", 0xff0000))
    bids_col.delete_many({"item_type": "duelist", "item_id": d["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Duelist Auction", f"{E_ARROW} **Player:** {d['username']}\n{E_MONEY} **Base:** ${d['base_price']:,}", 0x9b59b6, thumbnail=d.get('avatar_url')))
    schedule_auction_timer("duelist", d["id"], ctx.channel.id)

@bot.hybrid_command(name="deleteclub", description="Admin: Delete a club.")
@commands.has_permissions(administrator=True)
async def deleteclub(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    clubs_col.delete_one({"id": c['id']})
    history_col.delete_many({"club_id": c['id']})
    duelists_col.update_many({"club_id": c['id']}, {"$set": {"club_id": None, "owned_by": None}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deleted", f"Club **{club_name}** removed.", 0xff0000))

@bot.hybrid_command(name="setprefix", description="Admin: Change command prefix.")
@commands.has_permissions(administrator=True)
async def setprefix(ctx, new_prefix: str):
    config_col.update_one({"key": "prefix"}, {"$set": {"value": new_prefix}}, upsert=True)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Prefix Updated", f"New prefix: **`{new_prefix}`**", 0x2ecc71))

@bot.hybrid_command(name="admin_reset_all", description="Owner: Reset entire season data.")
@commands.has_permissions(administrator=True)
async def admin_reset_all(ctx):
    if BOT_OWNER_ID and ctx.author.id != BOT_OWNER_ID: return
    await ctx.send(embed=create_embed(f"{E_DANGER} WARNING", "Resetting EVERYTHING...", 0xff0000))
    clubs_col.update_many({}, {"$set": {"total_wins": 0, "level_name": LEVEL_UP_CONFIG[0][1], "owner_id": None, "value": 1000000}})
    battles_col.delete_many({})
    history_col.delete_many({})
    profiles_col.update_many({}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
    duelists_col.update_many({}, {"$set": {"owned_by": None, "club_id": None}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Reset", "System Reset Complete.", 0x2ecc71))

@bot.hybrid_command(name="forcewinner", description="Owner: Force end an auction.")
@commands.has_permissions(administrator=True)
async def forcewinner(ctx, item_type: str, item_id: int, winner_str: str, amount: int):
    bids_col.insert_one({"bidder": winner_str, "amount": amount, "item_type": item_type, "item_id": int(item_id)})
    await finalize_auction(item_type, int(item_id), ctx.channel.id)
    await ctx.send(embed=create_embed(f"{E_ADMIN} Force Win", f"Forced winner **{winner_str}**.", 0xe67e22))

@bot.hybrid_command(name="freezeauction", description="Owner: Freeze all auctions.")
@commands.has_permissions(administrator=True)
async def freezeauction(ctx):
    global bidding_frozen
    bidding_frozen = True
    await ctx.send(embed=create_embed(f"{E_DANGER} Frozen", "Auctions frozen.", 0xff0000))

@bot.hybrid_command(name="unfreezeauction", description="Owner: Resume auctions.")
@commands.has_permissions(administrator=True)
async def unfreezeauction(ctx):
    global bidding_frozen
    bidding_frozen = False
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Unfrozen", "Auctions resumed.", 0x2ecc71))

@bot.hybrid_command(name="registerbattle", description="Admin: Create a match.")
@commands.has_permissions(administrator=True)
async def registerbattle(ctx, club_a_name: str, club_b_name: str):
    ca = clubs_col.find_one({"name": {"$regex": f"^{club_a_name}$", "$options": "i"}})
    cb = clubs_col.find_one({"name": {"$regex": f"^{club_b_name}$", "$options": "i"}})
    if not ca or not cb: return await ctx.send(embed=create_embed("Error", "Clubs not found.", 0xff0000))
    bid = get_next_id("battle_id")
    battles_col.insert_one({"id": bid, "club_a": ca['id'], "club_b": cb['id'], "status": "REGISTERED"})
    await ctx.send(embed=create_embed(f"{E_FIRE} Battle Ready", f"**{ca['name']}** vs **{cb['name']}**\nID: {bid}", 0xe74c3c))

@bot.hybrid_command(name="battleresult", description="Admin: Log match result.")
@commands.has_permissions(administrator=True)
async def battleresult(ctx, battle_id: int, winner_name: str):
    b = battles_col.find_one({"id": int(battle_id)})
    if not b: return await ctx.send(embed=create_embed("Error", "Battle not found.", 0xff0000))
    wc = clubs_col.find_one({"name": {"$regex": f"^{winner_name}$", "$options": "i"}})
    if not wc: return await ctx.send(embed=create_embed("Error", "Winner not found.", 0xff0000))
    
    loser_id = b['club_a'] if b['club_b'] == wc['id'] else b['club_b']
    lc = clubs_col.find_one({"id": loser_id})
    
    clubs_col.update_one({"id": wc['id']}, {"$inc": {"value": WIN_VALUE_BONUS}})
    clubs_col.update_one({"id": loser_id}, {"$inc": {"value": LOSS_VALUE_PENALTY}})
    battles_col.update_one({"id": int(battle_id)}, {"$set": {"status": "COMPLETED"}})
    update_club_level(wc['id'], 1)
    
    # Generate Banter & Log
    banter = random.choice(BATTLE_BANTER)
    winner_emoji = resolve_emoji(random.choice(WINNER_REACTIONS))
    loser_emoji = resolve_emoji(random.choice(LOSER_REACTIONS))
    
    embed_log = create_embed(
        f"{E_FIRE} Match Result", 
        f"{E_WINNER_TROPHY} **Winner:** {wc['name']} {winner_emoji}\n"
        f"{E_LOSER_MARK} **Loser:** {lc['name']} {loser_emoji}\n\n"
        f"_{banter.replace('Team A', wc['name']).replace('Team B', lc['name'])}_", 
        0xe74c3c
    )
    await send_log("battle", embed_log)
    
    await ctx.send(embed=embed_log)

@bot.hybrid_command(name="checkclubmessages", description="Admin: Manual activity bonus.")
@commands.has_permissions(administrator=True)
async def checkclubmessages(ctx, club_name: str, count: int):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    bonus = (count // OWNER_MSG_COUNT_PER_BONUS) * OWNER_MSG_VALUE_BONUS
    if bonus > 0:
        clubs_col.update_one({"id": c['id']}, {"$inc": {"value": bonus}})
        await ctx.send(embed=create_embed(f"{E_BOOST} Activity Bonus", f"**{c['name']}** value increased by **${bonus:,}**.", 0x2ecc71))
    else: await ctx.send(embed=create_embed("Info", "Not enough messages.", 0x95a5a6))

@bot.hybrid_command(name="adjustgroupfunds", description="Admin: Cheat funds to group.")
@commands.has_permissions(administrator=True)
async def adjustgroupfunds(ctx, group_name: str, amount: int):
    groups_col.update_one({"name": group_name.lower()}, {"$inc": {"funds": amount}})
    await ctx.send(embed=create_embed(f"{E_ADMIN} Funds Adjusted", f"Adjusted **{group_name}** by ${amount:,}.", 0xe67e22))

@bot.hybrid_command(name="auditlog", description="Owner: View logs.")
async def auditlog(ctx, lines: int = 10):
    logs = list(audit_col.find().sort("timestamp", -1).limit(lines))
    txt = "\n".join([f"[{l['timestamp'].strftime('%H:%M')}] {l['entry']}" for l in logs])
    await ctx.send(f"```{txt}```")

@bot.hybrid_command(name="resetauction", description="Owner: Clear bids.")
async def resetauction(ctx):
    bids_col.delete_many({})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Reset", "Bids cleared.", 0x2ecc71))

@bot.hybrid_command(name="transferclub", description="Admin: Force transfer club.")
@commands.has_permissions(administrator=True)
async def transferclub(ctx, old_grp: str, new_grp: str):
    c = clubs_col.find_one({"owner_id": f"group:{old_grp.lower()}"})
    if c:
        clubs_col.update_one({"id": c['id']}, {"$set": {"owner_id": f"group:{new_grp.lower()}"}})
        await ctx.send(embed=create_embed(f"{E_ADMIN} Transferred", f"Club transferred to {new_grp}.", 0xe67e22))
    else: await ctx.send(embed=create_embed("Error", "No club found.", 0xff0000))

@bot.hybrid_command(name="tip", description="Admin: Add money to user.")
@commands.has_permissions(administrator=True)
async def tip(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": amount}}, upsert=True)
    log_user_activity(member.id, "Transaction", f"Received tip of ${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Tip", f"Added **${amount:,}** to {member.mention}.", 0xe67e22))

@bot.hybrid_command(name="deduct_user", description="Admin: Remove money from user.")
@commands.has_permissions(administrator=True)
async def deduct_user(ctx, member: discord.Member, amount: int):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    log_user_activity(member.id, "Transaction", f"Deducted ${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Deduct", f"Removed **${amount:,}** from {member.mention}.", 0xff0000))

@bot.hybrid_command(name="setclubmanager", description="Admin: Set club manager.")
@commands.has_permissions(administrator=True)
async def setclubmanager(ctx, club_name: str, member: discord.Member):
    clubs_col.update_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}}, {"$set": {"manager_id": str(member.id)}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Manager Set", f"{member.mention} is now manager of {club_name}.", 0x2ecc71))

@bot.hybrid_command(name="logpayment", description="Admin: Manually log a payment.")
@commands.has_permissions(administrator=True)
async def logpayment(ctx, user: discord.Member, amount: int, *, reason: str):
    embed = create_embed(
        f"{E_ADMIN} Payment Log", 
        f"**From:** {ctx.author.mention}\n**To:** {user.mention}\n**Amount:** ${amount:,}\n**Reason:** {reason}\n**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}", 
        0x9b59b6
    )
    await send_log("withdraw", embed)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Logged", "Logged.", 0x2ecc71))

# ---------- HELP ----------
@bot.hybrid_command(name="helpme", description="Show commands.")
async def helpme(ctx):
    p = get_prefix(bot, ctx.message)
    e1 = discord.Embed(title=f"{E_MONEY} User & Economy", color=0x2ecc71)
    e1.add_field(name="Basic", value=f"`{p}profile`\n`{p}wallet`\n`{p}withdrawwallet`", inline=True)
    e1.add_field(name="Group", value=f"`{p}creategroup`\n`{p}joingroup`\n`{p}deposit`\n`{p}groupinfo`\n`{p}grouplist`", inline=True)
    await ctx.send(embed=e1)
    e2 = discord.Embed(title=f"{E_AUCTION} Market & Bids", color=0xe67e22)
    e2.add_field(name="Bidding", value=f"`{p}placebid`\n`{p}groupbid`\n`{p}sellclub`\n`{p}sellshares`", inline=True)
    e2.add_field(name="Info", value=f"`{p}marketpanel`\n`{p}clubinfo`\n`{p}leaderboard`\n`{p}listclubs`", inline=True)
    await ctx.send(embed=e2)
    e3 = discord.Embed(title=f"{E_ADMIN} Admin Tools", color=0x95a5a6)
    e3.add_field(name="Setup", value=f"`{p}registerclub`\n`{p}startclubauction`\n`{p}setprefix`\n`{p}logpayment`\n`{p}payout`", inline=True)
    e3.add_field(name="Actions", value=f"`{p}registerbattle`\n`{p}battleresult`\n`{p}tip`\n`{p}deduct_user`", inline=True)
    e3.add_field(name="Owner", value=f"`{p}admin_reset_all`\n`{p}forcewinner`\n`{p}freezeauction`\n`{p}adjustsalary`\n`{p}deductsalary`", inline=True)
    e3.add_field(name="History", value=f"`{p}playerhistory`", inline=True)
    await ctx.send(embed=e3)

# ---------- RUN ----------
@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} command(s)")
    except Exception as e:
        print(e)
    
    if not hasattr(bot, 'started'):
        bot.loop.create_task(market_simulation_task())
        bot.started = True

# Error Handling
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound): pass
    elif isinstance(error, commands.MissingRequiredArgument): await ctx.send(embed=create_embed("Error", f"Missing Argument: {error.param}", 0xff0000))
    elif isinstance(error, commands.MissingPermissions): await ctx.send(embed=create_embed("Error", "No Permission.", 0xff0000))
    else: print(error)

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)