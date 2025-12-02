# bot.py
# Full Club Auction Bot (Certified Final Production Build v2.1)
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
    "withdraw": 1443955732281167873, 
    "battle": 1439844034905374720,
    "club": 1443955856222851142,
    "duelist": 1443955967086690395
}

# Constants
TIME_LIMIT = 90 
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
    "<a:redfire1:1443251827490684938> Absolute demolition! **{winner}** tore **{loser}** apart like homework due tomorrow. <:e:1443996271990935552> What a massacre! {l_emoji}",
    "<a:miapikachu:1443253477533814865> **{winner}** owned the pitch today — sent **{loser}** home with a souvenir bag full of tears. {l_emoji} <:e:1443996214805790871>",
    "<a:cross2:972155180185452544> That wasn’t a match… that was a public execution. RIP **{loser}**, thoughts and prayers. <:e:1443996261941383178> {w_emoji}",
    "<a:red_dot:1443261605092786188> **{loser}** came to play football, **{winner}** came to play career-ending football. {w_emoji}",
    "<a:alert:1443254143308533863> That performance was so one-sided, even the referee felt bad for **{loser}**. <:e:1443996269113643028>",
    "<a:crownop:962190451744579605> **{winner}** delivered a masterclass — **{loser}** just attended the lecture. {w_emoji}",
    "<a:report:1443251629095649420> Someone call 911, we just witnessed a crime against **{loser}**. {l_emoji} <:e:1443996159080403174>",
    "<a:goldcheckmark:1443253229398917252> Fortress defended! **{winner}** protected their home like it was the national treasury. <:e:1443996171071914177>",
    "<a:boost:962277213204525086> Home crowd roaring, scoreboard soaring — **{winner}** showed **{loser}** the door. {l_emoji}",
    "<a:itembox:1443254784898367629> Welcome to our home — shoes off, ego off, points taken. Try again next time **{loser}**. {w_emoji}",
    "<a:text:1443251311939293267> Silence in the stadium — **{winner}** just robbed **{loser}** in their own house. <:e:1443996142382612662> {w_emoji}",
    "<a:geeen_dot:1443252917648752681> **{winner}** turned **{loser}**'s home ground into a training session. {l_emoji} <:e:1443996188951973999>",
    "<a:redarrow:1443251741905653811> Fans left early, stadium empty — **{winner}** evacuated **{loser}**'s hopes. {w_emoji}",
    "<a:yellowstar:1443252221645950996> From hopeless to ruthless — **{winner}** just turned the match into a movie! <:e:1443996193012187217>",
    "<a:rules:1443252031220613321> Heartbreak for **{loser}**! Just when they thought they had it, **{winner}** said plot twist. {l_emoji}",
    "<a:bluestars:1443254349869486140> Comeback kings! **{winner}** woke up and chose violence. <:e:1443996181733834803> {w_emoji}",
    "<a:cross2:972155180185452544> **{loser}** tried their best, but sometimes best is not enough. {l_emoji} <:e:1443996139362844783>",
    "<a:red_dot:1443261605092786188> Fought hard, but went home harder — rough night for **{loser}**. {l_emoji}",
    "<a:alert:1443254143308533863> Painful defeat — that last moment hit **{loser}** harder than a tax bill. <:e:1443996248871928090> {l_emoji}",
    "<a:NyanCat:1443253686771126454> **{loser}** bottled it like a soda factory. {w_emoji}",
    "<a:donate_red:1443252440634884117> They had the lead… and then **{loser}** donated it to **{winner}**. {l_emoji}",
    "<a:arrow_arrow:962945777821450270> That collapse by **{loser}** was sponsored by gravity. {w_emoji}",
    "<a:verified:962942818886770688> A draw that felt like a loss for **{loser}** and a rescue mission for **{winner}**. {l_emoji}",
    "<a:1031pixelclock:1443253900793741332> Balanced scoreline, unbalanced emotions. {w_emoji} {l_emoji}"
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
E_LOSER_MARK = "<a:kids:960920729300377700>"

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

# Donor Weights for Giveaways
DONOR_WEIGHTS = {
    972809181444861984: 1,
    972809182224994354: 1,
    972809183374225478: 2,
    972809180966703176: 2,
    972809183718150144: 4,
    972809184242434048: 8,
    973502021757968414: 12
}

# ---------- DATABASE CONNECTION ----------
if not MONGO_URL:
    print("CRITICAL: MONGO_URL missing.")
    cluster = None
    db = None
else:
    cluster = MongoClient(MONGO_URL, tlsCAFile=certifi.where())
    db = cluster["auction_bot"]

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
    pending_deals_col = db.pending_deals

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

class HumanInt(commands.Converter):
    async def convert(self, ctx, argument):
        try:
            clean = argument.lower().replace(",", "").replace("$", "")
            if "k" in clean:
                return int(float(clean.replace("k", "")) * 1000)
            if "m" in clean:
                return int(float(clean.replace("m", "")) * 1000000)
            if "b" in clean:
                return int(float(clean.replace("b", "")) * 1000000000)
            return int(clean)
        except:
            raise commands.BadArgument(f"Invalid number: {argument}")

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

class ParticipantView(discord.ui.View):
    def __init__(self, message_id, required_roles=None):
        super().__init__(timeout=None)
        self.message_id = message_id
        self.required_roles = required_roles

    @discord.ui.button(label="Check Participants", style=discord.ButtonStyle.gray, emoji="👀")
    async def check_list(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message(f"{E_ERROR} Admins only.", ephemeral=True)

        message = await interaction.channel.fetch_message(self.message_id)
        reaction = None
        emoji_to_check = "🎉"
        if E_GIVEAWAY.startswith("<"): emoji_to_check = E_GIVEAWAY 

        for r in message.reactions:
             if str(r.emoji) == str(emoji_to_check) or str(r.emoji) == "🎉":
                 reaction = r
                 break
        
        if not reaction: return await interaction.response.send_message("No entries yet.", ephemeral=True)

        users = [user async for user in reaction.users() if not user.bot]
        valid_participants = []
        
        for u in users:
            if isinstance(u, discord.Member):
                if self.required_roles:
                    user_role_ids = [r.id for r in u.roles]
                    if isinstance(self.required_roles, list): 
                        if any(rid in self.required_roles for rid in user_role_ids):
                            valid_participants.append(f"• {u.display_name}") 
                    elif isinstance(self.required_roles, int):
                         if self.required_roles in user_role_ids:
                             valid_participants.append(f"• {u.display_name}")
                else:
                    valid_participants.append(f"• {u.display_name}")

        count = len(valid_participants)
        text = "\n".join(valid_participants[:40])
        if count > 40: text += f"\n...and {count-40} more."
        if count == 0: text = "No valid entries found."
        await interaction.response.send_message(f"**Valid Entries:** {count}\n\n{text}", ephemeral=True)

class HelpView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def send_category_embed(self, interaction, title, description, fields, color):
        embed = discord.Embed(title=title, description=description, color=color)
        for name, val in fields:
            embed.add_field(name=name, value=val, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Economy & Groups", style=discord.ButtonStyle.success, emoji=discord.PartialEmoji.from_str(E_MONEY))
    async def economy_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        fields = [
            (f"{E_MONEY} **Personal Wallet**", f"**Commands:** `.wallet` or `.wl`\n**What is it?** Check your cash balance.\n**Why use it?** See if you can afford items.\n**How:** Type `.wl`"),
            (f"{E_PREMIUM} **Investment Groups**", f"**Commands:** `.groupinfo` or `.gi`\n**What is it?** A shared bank account for you and your friends.\n**Why use it?** Pool funds to buy expensive clubs together.\n**How:** `.gi <Name>`"),
            (f"{E_BOOST} **Deposit/Withdraw**", f"**Commands:** `.deposit` / `.withdraw`\n**What is it?** Move money between your wallet and group.\n**How:** `.dep <Group> <Amount>`")
        ]
        await self.send_category_embed(interaction, f"{E_MONEY} Economy Guide", "Manage your finances.", fields, 0x2ecc71)

    @discord.ui.button(label="Football Features", style=discord.ButtonStyle.primary, emoji=discord.PartialEmoji.from_str(E_FIRE))
    async def football_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        fields = [
            (f"{E_CROWN} **Club Info**", f"**Commands:** `.clubinfo` or `.ci`\n**What is it?** Full stats for any club (Owner, Value, Wins).\n**Why use it?** Research before buying.\n**How:** `.ci Real Madrid`"),
            (f"{E_BOOST} **Leaderboards**", f"**Commands:** `.leaderboard` or `.lb`\n**What is it?** Ranking of top clubs.\n**Why use it?** See who is the best manager.\n**How:** `.lb`"),
            (f"{E_ITEMBOX} **Duelist Registry**", f"**Commands:** `.listduelists` or `.ld`\n**What is it?** List of players available for hire.")
        ]
        await self.send_category_embed(interaction, f"{E_STAR} Football Management", "Clubs, Stats & Matches.", fields, 0x3498db)

    @discord.ui.button(label="Market & Buying", style=discord.ButtonStyle.secondary, emoji=discord.PartialEmoji.from_str(E_AUCTION))
    async def market_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        fields = [
            (f"{E_AUCTION} **Market List**", f"**Commands:** `.marketlist` or `.ml`\n**What is it?** Live list of unsold clubs.\n**Why use it?** Buy clubs instantly.\n**How:** `.ml`"),
            (f"{E_MONEY} **Buy Club**", f"**Commands:** `.buyclub` or `.bc`\n**What is it?** Request to buy a club with your funds.\n**Note:** Requires Admin Approval. Refunded if rejected.\n**How:** `.bc <ClubName>`"),
            (f"{E_TIMER} **Auctions**", f"**Commands:** `.placebid` or `.pb`\n**What is it?** Bid on rare items.\n**How:** `.pb <Amount> <Type> <ID>`")
        ]
        await self.send_category_embed(interaction, f"{E_AUCTION} Transfer Market", "Buy, Sell & Trade.", fields, 0xe67e22)

    @discord.ui.button(label="Giveaways", style=discord.ButtonStyle.danger, emoji=discord.PartialEmoji.from_str(E_GIVEAWAY))
    async def giveaway_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        fields = [
            (f"{E_GIVEAWAY} **How to Enter**", f"React with 🎉 on giveaway messages."),
            (f"{E_BOOST} **Auto-Payouts**", f"Cash prizes are deposited to your wallet instantly."),
            (f"{E_CROWN} **Donor Perks**", f"Donors get **Multiplier Entries** (e.g., 12x chance) automatically.")
        ]
        await self.send_category_embed(interaction, f"{E_GIVEAWAY} Giveaway System", "Events & Rewards.", fields, 0xe74c3c)

    @discord.ui.button(label="Updates (v2.1)", style=discord.ButtonStyle.primary, emoji=discord.PartialEmoji.from_str(E_ADMIN))
    async def updates_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        desc = (
            f"{E_STARS} **Latest Features**\n{E_GOLD_TICK} **Short Commands:** Use `.bc` instead of `.buyclub`.\n{E_GOLD_TICK} **Human Numbers:** Type `1.5m` instead of `1500000`.\n{E_GOLD_TICK} **Secure Market:** New Approval System for purchases."
        )
        await self.send_category_embed(interaction, f"{E_ALERT} Patch Notes", desc, [], 0x9b59b6)

def create_embed(title, description, color=0x2ecc71, thumbnail=None, footer=None):
    embed = discord.Embed(title=title, description=description, color=color)
    if thumbnail and isinstance(thumbnail, str) and (thumbnail.startswith("http://") or thumbnail.startswith("https://")):
        embed.set_thumbnail(url=thumbnail)
    if footer: embed.set_footer(text=footer)
    return embed

def resolve_emoji(item):
    if isinstance(item, int) or (isinstance(item, str) and item.isdigit()):
        e = bot.get_emoji(int(item))
        if e: return str(e)
        return f"<:e:{item}>"
    if isinstance(item, str) and item.startswith(":") and item.endswith(":"):
        name = item.strip(":")
        e = discord.utils.get(bot.emojis, name=name)
        if e: return str(e)
        return item
    return str(item)

def log_audit(entry: str):
    if db is not None: audit_col.insert_one({"entry": entry, "timestamp": datetime.now()})

def log_user_activity(user_id, type, description):
    if db is not None:
        activities_col.insert_one({
            "user_id": str(user_id), "type": type, "description": description, "timestamp": datetime.now()
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
    if not c or "owner_id" not in c or not c["owner_id"]: return None, []
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
        "user_id": str(user_id), "type": type, "name": name, "timestamp": datetime.now()
    })

def parse_duration(time_str):
    time_str = time_str.lower()
    units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
    try:
        unit = time_str[-1]
        val = int(time_str[:-1])
        return val * units.get(unit, 60)
    except: return 60

def parse_prize_amount(prize_str):
    try:
        clean = prize_str.lower().replace(",", "").replace("$", "")
        if "k" in clean: return int(float(clean.replace("k", "")) * 1000)
        if "m" in clean: return int(float(clean.replace("m", "")) * 1000000)
        if clean.isdigit(): return int(clean)
    except: return 0
    return 0

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

async def finalize_auction(item_type: str, item_id: int, channel_id: int):
    if db is None: return
    winner_bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
    channel = bot.get_channel(channel_id)
    club_item = clubs_col.find_one({"id": int(item_id)}) if item_type == "club" else None
    
    if winner_bid:
        bidder_str = winner_bid["bidder"]
        amount = int(winner_bid["amount"])
        
        if bidder_str.startswith('group:'):
            gname = bidder_str.replace('group:', '').lower()
            groups_col.update_one({"name": gname}, {"$inc": {"funds": -amount}})
        else:
            wallets_col.update_one({"user_id": bidder_str}, {"$inc": {"balance": -amount}})
            log_user_activity(bidder_str, "Transaction", f"Paid ${amount:,} for Auction {item_type} {item_id}")
            
        if item_type == "club":
            old_owner = club_item.get("owner_id")
            if old_owner and not old_owner.startswith("group:"):
                profiles_col.update_one({"user_id": old_owner}, {"$unset": {"owned_club_id": "", "owned_club_share": ""}})
                log_past_entity(old_owner, "ex_owner", club_item["name"])

            history_col.insert_one({"club_id": int(item_id), "winner": bidder_str, "amount": amount, "timestamp": datetime.now(), "market_value_at_sale": club_item.get("value", 0)})
            clubs_col.update_one({"id": int(item_id)}, {
                "$set": { "owner_id": bidder_str, "last_bid_price": amount, "value": amount, "ex_owner_id": old_owner }
            })
            
            if not bidder_str.startswith('group:'):
                profiles_col.update_one({"user_id": bidder_str}, {"$set": {"owned_club_id": int(item_id), "owned_club_share": 100}}, upsert=True)
                log_user_activity(bidder_str, "Win", f"Won Auction for Club {club_item['name']}")
            
            if channel:
                await channel.send(embed=create_embed(f"{E_GIVEAWAY} AUCTION SOLD", f"{E_SUCCESS} **New Owner:** {bidder_str}\n{E_ITEMBOX} **Club:** {club_item['name']}\n{E_MONEY} **Final Price:** ${amount:,}\n{E_STARS} **New Market Value:** ${amount:,}", 0xf1c40f, thumbnail=club_item.get("logo")))
        else: 
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
            wallets_col.update_one({"user_id": d_item["discord_user_id"]}, {"$inc": {"balance": amount}}, upsert=True)
            log_user_activity(d_item["discord_user_id"], "Transaction", f"Received ${amount:,} Signing Fee.")
            if channel:
                 await channel.send(embed=create_embed(f"{E_GIVEAWAY} DUELIST SIGNED", f"{E_SUCCESS} **Signed To:** {bidder_str}\n{E_ITEMBOX} **Player:** {d_item['username']}\n{E_MONEY} **Transfer Fee:** ${amount:,}", 0x9b59b6, thumbnail=d_item.get('avatar_url')))
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

@bot.event
async def on_command_completion(ctx):
    log_user_activity(ctx.author.id, "Command", f"Used {E_CHAT} `.{ctx.command.name}`")

# ===========================
#   GROUP 1: USER & ECONOMY
# ===========================

@bot.hybrid_command(name="playerhistory", aliases=["ph"], description="Admin: View full user history.")
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

@bot.hybrid_command(name="profile", aliases=["pr"], description="View profile stats.")
async def profile(ctx, member: discord.Member = None):
    member = member or ctx.author
    uid = str(member.id)
    w = wallets_col.find_one({"user_id": uid})
    bal = w["balance"] if w else 0
    thumbnail_url = member.avatar.url if member.avatar else None
    group_mem = group_members_col.find_one({"user_id": uid})
    if group_mem:
        g_info = groups_col.find_one({"name": group_mem['group_name']})
        if g_info and g_info.get('logo'): thumbnail_url = g_info['logo']
    embed = create_embed(f"{E_CROWN} Profile", f"**User:** {member.mention}\n{E_MONEY} **Balance:** ${bal:,}", 0x3498db, thumbnail=thumbnail_url)
    groups = list(group_members_col.find({"user_id": uid}))
    g_list = [f"{g['group_name'].title()} ({g['share_percentage']}%)" for g in groups]
    embed.add_field(name="Groups", value=", ".join(g_list) if g_list else "None", inline=False)
    prof = profiles_col.find_one({"user_id": uid})
    if prof and prof.get("owned_club_id"):
        c = clubs_col.find_one({"id": prof["owned_club_id"]})
        if c: embed.add_field(name="Owned Club", value=f"{c['name']} (100%)", inline=False)
    duelist = duelists_col.find_one({"discord_user_id": uid})
    if duelist:
        cname = "Free Agent"
        if duelist.get("club_id"):
            c = clubs_col.find_one({"id": duelist["club_id"]})
            if c: cname = c["name"]
        embed.add_field(name="Duelist Status", value=f"Club: {cname}\nSalary: ${duelist['expected_salary']:,}", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="wallet", aliases=["wl"], description="Check your balance.")
async def wallet(ctx):
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    bal = w["balance"] if w else 0
    embed = create_embed(f"{E_MONEY} {E_NYAN} Wallet Balance", f"**User:** {ctx.author.mention}\n**Cash:** ${bal:,}", 0x2ecc71, thumbnail=ctx.author.avatar.url if ctx.author.avatar else None)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="payout", aliases=["po"], description="Admin: Pay a user from system.")
@commands.has_permissions(administrator=True)
async def payout(ctx, user: discord.Member, amount: HumanInt, *, reason: str):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(user.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(user.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(user.id, "Payout", f"Cashed out ${amount:,} by {ctx.author.name}. Reason: {reason}")
    embed_log = create_embed(f"{E_MONEY} Payout Log", f"**Paid To:** {user.mention}\n**Paid By:** {ctx.author.mention}\n**Amount:** ${amount:,}\n**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M')}\n**Reason:** {reason}", 0xe74c3c)
    await send_log("withdraw", embed_log)
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Payout Successful", f"Processed payout of **${amount:,}** for {user.mention}.", 0x2ecc71))

@bot.hybrid_command(name="withdrawwallet", aliases=["ww"], description="Burn money from wallet.")
async def withdrawwallet(ctx, amount: HumanInt):
    if amount <= 0: return await ctx.send(embed=create_embed("Error", "Invalid amount.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Burned ${amount:,} from wallet.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdrawn", f"Removed **${amount:,}** from wallet.", 0x2ecc71))

@bot.hybrid_command(name="grouplist", aliases=["gl"], description="List all investor groups.")
async def grouplist(ctx):
    groups = list(groups_col.find())
    data = []
    for g in groups: data.append((g['name'].title(), f"{E_MONEY} ${g['funds']:,}"))
    view = Paginator(ctx, data, f"{E_PREMIUM} Group List", 0x9b59b6, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="groupinfo", aliases=["gi"], description="Get detailed info about a group.")
async def groupinfo(ctx, *, group_name: str):
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Group not found.", 0xff0000))
    members = list(group_members_col.find({"group_name": gname}))
    clubs = list(clubs_col.find({"owner_id": f"group:{gname}"}))
    embed = discord.Embed(title=f"{E_PREMIUM} Group: {g['name'].title()}", color=0x9b59b6)
    if g.get('logo'): embed.set_thumbnail(url=g['logo'])
    embed.add_field(name="Bank", value=f"{E_MONEY} ${g['funds']:,}", inline=True)
    mlist = []
    for m in members[:15]:
        try: u = await bot.fetch_user(int(m['user_id'])); name = u.name
        except: name = "Unknown"
        mlist.append(f"{E_ARROW} {name}: {m['share_percentage']}%")
    if len(members) > 15: mlist.append(f"...and {len(members)-15} more.")
    embed.add_field(name=f"Members ({len(members)})", value="\n".join(mlist) or "None", inline=False)
    clist = [c['name'] for c in clubs]
    embed.add_field(name="Clubs Owned", value=", ".join(clist) or "None", inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="creategroup", aliases=["cg"], description="Create a new investment group.")
async def creategroup(ctx, name: str, share: int):
    gname = name.lower()
    if groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group exists.", 0xff0000))
    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    groups_col.insert_one({"name": gname, "funds": 0, "owner_id": str(ctx.author.id), "logo": logo_url})
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Created group {name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Created", f"Group **{name}** created with **{share}%** share.", 0x2ecc71, thumbnail=logo_url))

@bot.hybrid_command(name="joingroup", aliases=["jg"], description="Join an existing group.")
async def joingroup(ctx, name: str, share: int):
    gname = name.lower()
    if not groups_col.find_one({"name": gname}): return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Already a member.", 0xff0000))
    group_members_col.insert_one({"group_name": gname, "user_id": str(ctx.author.id), "share_percentage": share})
    log_user_activity(ctx.author.id, "Group", f"Joined group {name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Joined", f"Joined **{name}** with **{share}%**.", 0x2ecc71))

@bot.hybrid_command(name="deposit", aliases=["dep"], description="Transfer funds to group.")
async def deposit(ctx, group_name: str, amount: HumanInt):
    if amount <= 0: return
    gname = group_name.lower()
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Not a member.", 0xff0000))
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    groups_col.update_one({"name": gname}, {"$inc": {"funds": amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Deposited ${amount:,} to {group_name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deposit", f"Deposited **${amount:,}** to **{group_name}**.", 0x2ecc71))

@bot.hybrid_command(name="withdraw", aliases=["wd"], description="Withdraw funds from group.")
async def withdraw(ctx, group_name: str, amount: HumanInt):
    gname = group_name.lower()
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    g = groups_col.find_one({"name": gname})
    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -amount}})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": amount}})
    log_user_activity(ctx.author.id, "Transaction", f"Withdrew ${amount:,} from {group_name}.")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdraw", f"Withdrew **${amount:,}**.", 0x2ecc71))

@bot.hybrid_command(name="leavegroup", aliases=["lg"], description="Leave a group.")
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

# ===========================
#   GROUP 2: MARKET & BIDS
# ===========================

@bot.hybrid_command(name="placebid", aliases=["pb"], description="Place a bid.")
async def placebid(ctx, amount: HumanInt, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", f"{E_DANGER} Auctions frozen.", 0xff0000))
    item_type = item_type.lower()
    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        is_active = (item_type, str(item_id)) in active_timers
        if d.get("owned_by") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This duelist is already signed.", 0xff0000))
        if not club_name: return await ctx.send(embed=create_embed("Error", "Provide club name.", 0xff0000))
        c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
        if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
        allowed = False
        if str(ctx.author.id) == c.get("owner_id"): allowed = True
        elif c.get("owner_id", "").startswith("group:"):
            gname = c.get("owner_id").replace("group:", "")
            if group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): allowed = True
        if not allowed: return await ctx.send(embed=create_embed("Error", "You/Group don't own this club.", 0xff0000))
    
    if item_type == "club":
         c = clubs_col.find_one({"id": int(item_id)})
         if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
         is_active = (item_type, str(item_id)) in active_timers
         if c.get("owner_id") and not is_active: return await ctx.send(embed=create_embed("Sold Out", f"{E_ERROR} This club is **SOLD OUT**. Wait for owner to sell.", 0xff0000))
         prof = profiles_col.find_one({"user_id": str(ctx.author.id)})
         if prof and prof.get("owned_club_id"): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You already own a club (100%). Sell it first.", 0xff0000))
    
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    req = min_required_bid(get_current_bid(item_type, item_id))
    if amount < req: return await ctx.send(embed=create_embed("Bid Error", f"Min bid is ${req:,}", 0xff0000))
    bids_col.insert_one({"bidder": str(ctx.author.id), "amount": amount, "item_type": item_type, "item_id": int(item_id), "timestamp": datetime.now()})
    log_user_activity(ctx.author.id, "Bid", f"Placed bid of ${amount:,} on {item_type} {item_id}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Bid Placed", f"Bid of **${amount:,}** accepted.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="groupbid", aliases=["gb"], description="Place a bid using group funds.")
async def groupbid(ctx, group_name: str, amount: HumanInt, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send(embed=create_embed("Frozen", "Auctions frozen.", 0xff0000))
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Not member.", 0xff0000))
    
    if item_type == "club":
         c = clubs_col.find_one({"id": int(item_id)})
         is_active = (item_type, str(item_id)) in active_timers
         if c.get("owner_id") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This club is **SOLD OUT**.", 0xff0000))
         if clubs_col.find_one({"owner_id": f"group:{gname}"}): return await ctx.send(embed=create_embed("Error", "Group already owns a club.", 0xff0000))

    if item_type == "duelist":
        d = duelists_col.find_one({"id": int(item_id)})
        is_active = (item_type, str(item_id)) in active_timers
        if d.get("owned_by") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", f"{E_ERROR} This duelist is signed.", 0xff0000))
        if not club_name: return await ctx.send(embed=create_embed("Error", "Provide club name.", 0xff0000))
        c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
        if not c or c.get("owner_id") != f"group:{gname}": return await ctx.send(embed=create_embed("Error", "Group doesn't own club.", 0xff0000))
        
    if g["funds"] < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    bids_col.insert_one({"bidder": f"group:{gname}", "amount": amount, "item_type": item_type, "item_id": int(item_id)})
    log_user_activity(ctx.author.id, "Bid", f"Group bid ${amount:,} on {item_type} {item_id}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Bid", f"Group **{group_name}** bid **${amount:,}**.", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="sellclub", aliases=["sc"], description="Sell your club.")
async def sellclub(ctx, club_name: str, buyer: discord.Member = None):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    if str(ctx.author.id) != c.get("owner_id"): return await ctx.send(embed=create_embed("Error", "You don't own this.", 0xff0000))
    val = c["value"]
    target = buyer if buyer else "The Market"
    await ctx.send(embed=create_embed(f"{E_ALERT} Confirm Sale", f"Sell **{c['name']}** to {target.mention if buyer else 'Market'} for **${val:,}**?\nType `yes` or `no`.", 0xe67e22))
    try: msg = await bot.wait_for('message', check=lambda m: m.author == (buyer if buyer else ctx.author) and m.content.lower() in ['yes', 'no'], timeout=30.0)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    if msg.content.lower() == 'no': return await ctx.send(embed=create_embed("Info", "Cancelled.", 0x95a5a6))
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
    else:
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": None, "ex_owner_id": old_owner}})
        
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
    embed_log = create_embed(f"{E_ADMIN} Club Sold", f"**Club:** {c['name']}\n**Seller:** {ctx.author.mention}\n**Buyer:** {target.mention if buyer else 'Market'}\n**Price:** ${val:,}", 0xe67e22)
    await send_log("club", embed_log)
    log_user_activity(ctx.author.id, "Sale", f"Sold club {c['name']} for ${val:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", f"Club sold for **${val:,}**.", 0x2ecc71))

@bot.hybrid_command(name="sellshares", aliases=["ss"], description="Sell group shares.")
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
    try: msg = await bot.wait_for('message', check=lambda m: m.author == buyer and m.content.lower() in ['yes', 'no'], timeout=30)
    except: return await ctx.send(embed=create_embed("Info", "Timed out.", 0x95a5a6))
    if msg.content.lower() == 'yes':
        bw = wallets_col.find_one({"user_id": str(buyer.id)})
        if not bw or bw.get("balance", 0) < val: return await ctx.send(embed=create_embed("Error", "Buyer broke.", 0xff0000))
        wallets_col.update_one({"user_id": str(buyer.id)}, {"$inc": {"balance": -val}})
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": val}}, upsert=True)
        group_members_col.update_one({"_id": seller["_id"]}, {"$inc": {"share_percentage": -percentage}})
        group_members_col.update_one({"group_name": gname, "user_id": str(buyer.id)}, {"$inc": {"share_percentage": percentage}}, upsert=True)
        log_user_activity(ctx.author.id, "Sale", f"Sold {percentage}% shares of {gname}.")
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Sold", "Shares transferred.", 0x2ecc71))

@bot.hybrid_command(name="marketlist", aliases=["ml"], description="View unsold clubs.")
async def marketlist(ctx):
    unsold_clubs = list(clubs_col.find({"$or": [{"owner_id": None}, {"owner_id": ""}]}).sort("value", -1))
    if not unsold_clubs: return await ctx.send(embed=create_embed(f"{E_AUCTION} Market Empty", "All clubs are currently owned.", 0x95a5a6))
    data = []
    for c in unsold_clubs:
        data.append((f"{E_STAR} {c['name']}", f"{E_MONEY} **Price:** ${c['value']:,}\n{E_BOOST} **Division:** {c.get('level_name', 'Unknown')}\n{E_ITEMBOX} **ID:** {c['id']}"))
    view = Paginator(ctx, data, f"{E_AUCTION} Transfer Market", 0xe67e22, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="buyclub", aliases=["bc"], description="Request to buy a club.")
async def buyclub(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    if c.get("owner_id"): return await ctx.send(embed=create_embed("Error", f"{E_DANGER} Already owned.", 0xff0000))
    prof = profiles_col.find_one({"user_id": str(ctx.author.id)})
    if prof and prof.get("owned_club_id"): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} You already own a club.", 0xff0000))
    price = c["value"]
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < price: return await ctx.send(embed=create_embed("Insufficient Funds", f"{E_ERROR} Need **${price:,}**.", 0xff0000))
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -price}})
    deal_id = get_next_id("deal_id")
    pending_deals_col.insert_one({"id": deal_id, "type": "user", "buyer_id": str(ctx.author.id), "club_id": c["id"], "club_name": c["name"], "price": price, "timestamp": datetime.now()})
    embed = discord.Embed(title=f"{E_TIMER} Deal Pending", description=f"Request to buy **{c['name']}** submitted.\n\n{E_MONEY} **Funds Held:** ${price:,}\n{E_ADMIN} **Status:** Waiting for Admin Approval\n{E_ITEMBOX} **Deal ID:** {deal_id}", color=0xf1c40f)
    if c.get("logo"): embed.set_thumbnail(url=c["logo"])
    await ctx.send(embed=embed)

@bot.hybrid_command(name="groupbuyclub", aliases=["gbc"], description="Request to buy club for group.")
async def groupbuyclub(ctx, group_name: str, club_name: str):
    gname = group_name.lower()
    g = groups_col.find_one({"name": gname})
    if not g: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Group not found.", 0xff0000))
    if not group_members_col.find_one({"group_name": gname, "user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Not a member.", 0xff0000))
    if clubs_col.find_one({"owner_id": f"group:{gname}"}): return await ctx.send(embed=create_embed("Error", f"{E_DANGER} Group already owns a club.", 0xff0000))
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    if c.get("owner_id"): return await ctx.send(embed=create_embed("Error", f"{E_DANGER} Already owned.", 0xff0000))
    price = c["value"]
    if g.get("funds", 0) < price: return await ctx.send(embed=create_embed("Insufficient Funds", f"{E_ERROR} Group needs **${price:,}**.", 0xff0000))
    groups_col.update_one({"name": gname}, {"$inc": {"funds": -price}})
    deal_id = get_next_id("deal_id")
    pending_deals_col.insert_one({"id": deal_id, "type": "group", "buyer_id": f"group:{gname}", "initiator_id": str(ctx.author.id), "club_id": c["id"], "club_name": c["name"], "price": price, "timestamp": datetime.now()})
    embed = discord.Embed(title=f"{E_TIMER} Group Deal Pending", description=f"Request to buy **{c['name']}** for **{group_name}** submitted.\n\n{E_MONEY} **Funds Held:** ${price:,}\n{E_ADMIN} **Status:** Waiting for Admin Approval\n{E_ITEMBOX} **Deal ID:** {deal_id}", color=0xf1c40f)
    if c.get("logo"): embed.set_thumbnail(url=c["logo"])
    await ctx.send(embed=embed)

@bot.hybrid_command(name="checkdeals", aliases=["cd"], description="Admin: View pending deals.")
@commands.has_permissions(administrator=True)
async def checkdeals(ctx):
    deals = list(pending_deals_col.find().sort("timestamp", 1))
    if not deals: return await ctx.send(embed=create_embed(f"{E_SUCCESS} All Clear", "No pending deals.", 0x2ecc71))
    data = []
    for d in deals:
        buyer_display = d['buyer_id'].replace("group:", "Group: ").title() if "group:" in d['buyer_id'] else f"<@{d['buyer_id']}>"
        data.append((f"Deal #{d['id']} | {d['club_name']}", f"{E_MONEY} **Price:** ${d['price']:,}\n{E_CROWN} **Buyer:** {buyer_display}\n{E_TIMER} **Time:** {d['timestamp'].strftime('%Y-%m-%d %H:%M')}"))
    view = Paginator(ctx, data, f"{E_ADMIN} Pending Approvals", 0xe67e22, 5)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="managedeal", aliases=["md"], description="Admin: Approve/Reject deal.")
@commands.has_permissions(administrator=True)
async def managedeal(ctx, deal_id: int, action: str):
    action = action.lower()
    if action not in ["approve", "reject"]: return await ctx.send(embed=create_embed("Error", "Action must be `approve` or `reject`.", 0xff0000))
    deal = pending_deals_col.find_one({"id": deal_id})
    if not deal: return await ctx.send(embed=create_embed("Error", "Deal ID not found.", 0xff0000))
    c = clubs_col.find_one({"id": deal['club_id']})
    buyer_id = deal['buyer_id']
    price = deal['price']

    if action == "reject":
        if deal['type'] == "group":
            gname = buyer_id.replace("group:", "")
            groups_col.update_one({"name": gname}, {"$inc": {"funds": price}})
            try:
                initiator = await bot.fetch_user(int(deal['initiator_id']))
                await initiator.send(embed=create_embed(f"{E_DANGER} Deal Rejected", f"Your group request to buy **{deal['club_name']}** was rejected.\n{E_MONEY} **${price:,}** refunded.", 0xff0000))
            except: pass
        else:
            wallets_col.update_one({"user_id": buyer_id}, {"$inc": {"balance": price}})
            try:
                user = await bot.fetch_user(int(buyer_id))
                await user.send(embed=create_embed(f"{E_DANGER} Deal Rejected", f"Your request to buy **{deal['club_name']}** was rejected.\n{E_MONEY} **${price:,}** refunded.", 0xff0000))
            except: pass
        pending_deals_col.delete_one({"id": deal_id})
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Rejected", f"Deal #{deal_id} rejected. Funds refunded.", 0x2ecc71))
        return

    if action == "approve":
        if c.get("owner_id"):
            if deal['type'] == "group": groups_col.update_one({"name": buyer_id.replace("group:", "")}, {"$inc": {"funds": price}})
            else: wallets_col.update_one({"user_id": buyer_id}, {"$inc": {"balance": price}})
            pending_deals_col.delete_one({"id": deal_id})
            return await ctx.send(embed=create_embed("Error", "Club already owned! Refunded.", 0xff0000))
        clubs_col.update_one({"id": c["id"]}, {"$set": {"owner_id": buyer_id}})
        if deal['type'] == "user":
            profiles_col.update_one({"user_id": buyer_id}, {"$set": {"owned_club_id": c["id"], "owned_club_share": 100}}, upsert=True)
            log_user_activity(buyer_id, "Purchase", f"Bought {c['name']} (Approved)")
        else: log_user_activity(deal['initiator_id'], "Group Purchase", f"Group bought {c['name']} (Approved)")
        history_col.insert_one({"club_id": c["id"], "winner": buyer_id, "amount": price, "timestamp": datetime.now(), "type": "market_buy"})
        log_embed = create_embed(f"{E_GIVEAWAY} CLUB SOLD (Market)", f"Transfer Approved by {ctx.author.mention}\n\n{E_STAR} **Club:** {c['name']}\n{E_CROWN} **New Owner:** {buyer_id.replace('group:', 'Group: ').title() if 'group:' in buyer_id else f'<@{buyer_id}>'}\n{E_MONEY} **Price:** ${price:,}", 0xf1c40f)
        if c.get("logo"): log_embed.set_thumbnail(url=c['logo'])
        await send_log("club", log_embed)
        pending_deals_col.delete_one({"id": deal_id})
        await ctx.send(embed=create_embed(f"{E_SUCCESS} Approved", f"Deal #{deal_id} approved. Ownership transferred.", 0x2ecc71))

@bot.hybrid_command(name="listclubs", aliases=["lc"], description="List all registered clubs.")
async def listclubs(ctx):
    clubs = list(clubs_col.find().sort("value", -1))
    data = []
    for c in clubs: data.append((f"{E_STAR} {c['name']} (ID: {c['id']})", f"{E_MONEY} ${c['value']:,} | {E_BOOST} {c.get('level_name')} | {E_FIRE} Wins: {c.get('total_wins',0)}"))
    view = Paginator(ctx, data, f"{E_CROWN} Registered Clubs", 0x3498db, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="clubinfo", aliases=["ci"], description="Get club info.")
async def clubinfo(ctx, *, club_name_or_id: str):
    try: c = clubs_col.find_one({"id": int(club_name_or_id)})
    except: c = clubs_col.find_one({"name": {"$regex": f"^{club_name_or_id}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    owner_display = c.get('owner_id') or "Unowned"
    shareholder_text = ""
    if owner_display.startswith('group:'): 
        gname = owner_display.replace('group:', '').title()
        owner_display = f"Group: {gname}"
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
    manager_name = "None"
    if c['manager_id']:
        try: u = await bot.fetch_user(int(c['manager_id'])); manager_name = u.name
        except: pass
    duelists = list(duelists_col.find({"club_id": c['id']}))
    d_list = "\n".join([f"{E_ARROW} {d['username']}" for d in duelists]) or "None"
    embed = discord.Embed(title=f"{E_CROWN} {c['name']}", description=f"{E_BOOST} **{c.get('level_name')}**{shareholder_text}", color=0x3498db)
    if c.get("logo"): embed.set_thumbnail(url=c["logo"])
    embed.add_field(name="Owner", value=f"{E_STAR} {owner_display}", inline=True)
    embed.add_field(name="Value", value=f"{E_MONEY} ${c['value']:,}", inline=True)
    embed.add_field(name="Manager", value=f"{E_ADMIN} {manager_name}", inline=True)
    embed.add_field(name=f"{E_ITEMBOX} Duelists", value=d_list, inline=False)
    await ctx.send(embed=embed)

@bot.hybrid_command(name="leaderboard", aliases=["lb"], description="View top clubs.")
async def leaderboard(ctx):
    clubs = list(clubs_col.find().sort([("total_wins", -1), ("value", -1)]))
    data = []
    for i, c in enumerate(clubs): data.append((f"**{i+1}. {c['name']}**", f"{E_ARROW} {c.get('level_name')} | {E_FIRE} {c.get('total_wins')} Wins | {E_MONEY} ${c['value']:,}"))
    view = Paginator(ctx, data, f"{E_CROWN} Club Leaderboard", 0xf1c40f, 10)
    await ctx.send(embed=view.get_embed(), view=view)

# ===========================
#   GROUP 3: DUELIST
# ===========================

@bot.hybrid_command(name="registerduelist", aliases=["rd"], description="Register as duelist.")
async def registerduelist(ctx, username: str, base_price: HumanInt, salary: HumanInt):
    if duelists_col.find_one({"discord_user_id": str(ctx.author.id)}): return await ctx.send(embed=create_embed("Error", "Already registered.", 0xff0000))
    did = get_next_id("duelist_id")
    avatar = ctx.author.avatar.url if ctx.author.avatar else ""
    duelists_col.insert_one({"id": did, "discord_user_id": str(ctx.author.id), "username": username, "base_price": base_price, "expected_salary": salary, "avatar_url": avatar, "owned_by": None, "club_id": None})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Registered", f"Duelist **{username}** (ID: {did})", 0x9b59b6))

@bot.hybrid_command(name="listduelists", aliases=["ld"], description="List duelists.")
async def listduelists(ctx):
    ds = list(duelists_col.find())
    data = []
    for d in ds:
        cname = "Free Agent"
        if d.get("club_id"):
            c = clubs_col.find_one({"id": d["club_id"]})
            if c: cname = c["name"]
        data.append((f"{d['username']}", f"{E_ITEMBOX} ID: {d['id']}\n{E_MONEY} ${d['expected_salary']:,}\n{E_STAR} {cname}"))
    view = Paginator(ctx, data, f"{E_BOOK} Duelist Registry", 0x9b59b6, 10)
    await ctx.send(embed=view.get_embed(), view=view)

@bot.hybrid_command(name="adjustsalary", aliases=["as"], description="Owner: Bonus/Fine.")
async def adjustsalary(ctx, duelist_id: int, amount: HumanInt):
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
        wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": abs_amt}}, upsert=True)
        log_user_activity(ctx.author.id, "Transaction", f"Fined {d['username']} ${abs_amt:,}")
        await ctx.send(embed=create_embed(f"{E_DANGER} Fine", f"Deducted **${abs_amt:,}** from {d['username']}.", 0xff0000))

# ===========================
#   GROUP 4: ADMIN
# ===========================

@bot.hybrid_command(name="registerclub", aliases=["rc"], description="Admin: Register club.")
@commands.has_permissions(administrator=True)
async def registerclub(ctx, name: str, base_price: HumanInt, *, slogan: str = ""):
    if clubs_col.find_one({"name": {"$regex": f"^{name}$", "$options": "i"}}): return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club registered.", 0xff0000))
    logo_url = ctx.message.attachments[0].url if ctx.message.attachments else ""
    cid = get_next_id("club_id")
    clubs_col.insert_one({"id": cid, "name": name, "base_price": base_price, "value": base_price, "slogan": slogan, "logo": logo_url, "total_wins": 0, "level_name": LEVEL_UP_CONFIG[0][1], "owner_id": None, "manager_id": None})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Club Registered", f"{E_ARROW} **Name:** {name}\n{E_MONEY} **Base:** ${base_price:,}\n{E_ITEMBOX} **ID:** {cid}", 0x2ecc71, thumbnail=logo_url))

@bot.hybrid_command(name="startclubauction", aliases=["sca"], description="Admin: Start club auction.")
@commands.has_permissions(administrator=True)
async def startclubauction(ctx, club_name: str):
    c = clubs_col.find_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}})
    if not c: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Club not found.", 0xff0000))
    bids_col.delete_many({"item_type": "club", "item_id": c["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Auction Started", f"{E_ARROW} **Club:** {c['name']}\n{E_MONEY} **Base:** ${c['base_price']:,}", 0xe67e22, thumbnail=c.get('logo')))
    schedule_auction_timer("club", c["id"], ctx.channel.id)

@bot.hybrid_command(name="startduelistauction", aliases=["sda"], description="Admin: Start duelist auction.")
@commands.has_permissions(administrator=True)
async def startduelistauction(ctx, duelist_id: int):
    d = duelists_col.find_one({"id": int(duelist_id)})
    if not d: return await ctx.send(embed=create_embed("Error", f"{E_ERROR} Duelist not found.", 0xff0000))
    bids_col.delete_many({"item_type": "duelist", "item_id": d["id"]})
    await ctx.send(embed=create_embed(f"{E_AUCTION} Duelist Auction", f"{E_ARROW} **Player:** {d['username']}\n{E_MONEY} **Base:** ${d['base_price']:,}", 0x9b59b6, thumbnail=d.get('avatar_url')))
    schedule_auction_timer("duelist", d["id"], ctx.channel.id)

@bot.hybrid_command(name="registerbattle", aliases=["rb"], description="Admin: Create match.")
@commands.has_permissions(administrator=True)
async def registerbattle(ctx, club_a_name: str, club_b_name: str):
    ca = clubs_col.find_one({"name": {"$regex": f"^{club_a_name}$", "$options": "i"}})
    cb = clubs_col.find_one({"name": {"$regex": f"^{club_b_name}$", "$options": "i"}})
    if not ca or not cb: return await ctx.send(embed=create_embed("Error", "Clubs not found.", 0xff0000))
    bid = get_next_id("battle_id")
    battles_col.insert_one({"id": bid, "club_a": ca['id'], "club_b": cb['id'], "status": "REGISTERED"})
    await ctx.send(embed=create_embed(f"{E_FIRE} Battle Ready", f"**{ca['name']}** vs **{cb['name']}**\nID: {bid}", 0xe74c3c))

@bot.hybrid_command(name="battleresult", aliases=["br"], description="Admin: Log match result.")
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
    banter = random.choice(BATTLE_BANTER)
    winner_emoji = resolve_emoji(random.choice(WINNER_REACTIONS))
    loser_emoji = resolve_emoji(random.choice(LOSER_REACTIONS))
    final_banter = banter.format(winner=wc['name'], loser=lc['name'], w_emoji=winner_emoji, l_emoji=loser_emoji).replace("Team A", wc['name']).replace("Team B", lc['name'])
    embed_log = create_embed(f"{E_FIRE} Match Result", f"{resolve_emoji(E_WINNER_TROPHY)} **Winner:** {wc['name']} {winner_emoji}\n{resolve_emoji(E_LOSER_MARK)} **Loser:** {lc['name']} {loser_emoji}\n\n_{final_banter}_", 0xe74c3c)
    await send_log("battle", embed_log)
    await ctx.send(embed=embed_log)

@bot.hybrid_command(name="tip", aliases=["tp"], description="Admin: Add money to user.")
@commands.has_permissions(administrator=True)
async def tip(ctx, member: discord.Member, amount: HumanInt):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": amount}}, upsert=True)
    log_user_activity(member.id, "Transaction", f"Received tip of ${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Tip", f"Added **${amount:,}** to {member.mention}.", 0xe67e22))

@bot.hybrid_command(name="deduct_user", aliases=["du"], description="Admin: Deduct money.")
@commands.has_permissions(administrator=True)
async def deduct_user(ctx, member: discord.Member, amount: HumanInt):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    log_user_activity(member.id, "Transaction", f"Deducted ${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Deduct", f"Removed **${amount:,}** from {member.mention}.", 0xff0000))

@bot.hybrid_command(name="adjustgroupfunds", aliases=["agf"], description="Admin: Cheat funds.")
@commands.has_permissions(administrator=True)
async def adjustgroupfunds(ctx, group_name: str, amount: HumanInt):
    groups_col.update_one({"name": group_name.lower()}, {"$inc": {"funds": amount}})
    await ctx.send(embed=create_embed(f"{E_ADMIN} Funds Adjusted", f"Adjusted **{group_name}** by ${amount:,}.", 0xe67e22))

@bot.hybrid_command(name="admin_reset_all", description="Owner: Reset EVERYTHING.")
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

# ===========================
#   GROUP 5: GIVEAWAYS
# ===========================

async def run_giveaway(ctx, prize, winners_count, duration_seconds, description, required_role_ids=None, weighted=False, image_url=None):
    end_time = int(datetime.now().timestamp() + duration_seconds)
    embed = discord.Embed(title=f"{E_GIVEAWAY} {prize}", description=description, color=0xe74c3c)
    embed.add_field(name="Timer", value=f"{E_TIMER} Ends <t:{end_time}:R>", inline=True)
    embed.add_field(name="Winners", value=f"{E_CROWN} {winners_count}", inline=True)
    embed.add_field(name="Host", value=f"{E_ADMIN} {ctx.author.mention}", inline=True)
    if image_url: embed.set_image(url=image_url)
    embed.set_footer(text="React with 🎉 to enter!")
    msg = await ctx.send(embed=embed)
    try: await msg.add_reaction("🎉") 
    except: pass
    view = ParticipantView(msg.id, required_role_ids)
    await msg.edit(view=view)
    await asyncio.sleep(duration_seconds)
    try: msg = await ctx.channel.fetch_message(msg.id)
    except: return
    reaction = discord.utils.get(msg.reactions, emoji="🎉")
    users = []
    if reaction:
        async for user in reaction.users():
            if not user.bot:
                member = ctx.guild.get_member(user.id)
                if not member: continue
                if required_role_ids:
                    user_role_ids = [r.id for r in member.roles]
                    if isinstance(required_role_ids, list):
                        has_role = False
                        weight = 0
                        for rid, w in DONOR_WEIGHTS.items():
                            if rid in user_role_ids:
                                has_role = True
                                if weighted: weight = max(weight, w)
                                else: weight = 1
                        if has_role:
                            for _ in range(weight): users.append(member)
                    elif isinstance(required_role_ids, int):
                         if required_role_ids in user_role_ids: users.append(member)
                else: users.append(member)
    if not users:
        fail_embed = create_embed(f"{E_GIVEAWAY} Ended", f"Prize: **{prize}**\n\nNo valid entries.", 0x95a5a6)
        if image_url: fail_embed.set_image(url=image_url)
        return await msg.reply(embed=fail_embed)
    final_winners = []
    unique_check = []
    for _ in range(winners_count):
        if not users: break
        winner = random.choice(users)
        if winner.id not in unique_check:
            final_winners.append(winner)
            unique_check.append(winner.id)
        users = [u for u in users if u.id != winner.id]
    winner_mentions = ", ".join([w.mention for w in final_winners])
    tip_amount = parse_prize_amount(prize)
    tip_msg = ""
    if tip_amount > 0:
        for w in final_winners:
             wallets_col.update_one({"user_id": str(w.id)}, {"$inc": {"balance": tip_amount}}, upsert=True)
             log_user_activity(w.id, "Giveaway", f"Won giveaway: {prize} (+${tip_amount:,})")
        tip_msg = f"\n\n{E_MONEY} **Auto-Tip:** ${tip_amount:,} has been sent to the winner(s)!"
    win_embed = discord.Embed(title=f"{E_GIVEAWAY} GIVEAWAY ENDED", description=f"**Prize:** {prize}\n\n{E_CROWN} **Winner(s):** {winner_mentions}{tip_msg}", color=0xf1c40f)
    if image_url: win_embed.set_image(url=image_url)
    win_embed.set_footer(text=f"Hosted by {ctx.author.display_name}")
    await msg.reply(f"Congratulations {winner_mentions}!", embed=win_embed)

@bot.hybrid_command(name="giveaway_donor", description="Admin: Start Donor giveaway.")
@commands.has_permissions(administrator=True)
async def giveaway_donor(ctx, prize: str, winners: int, duration: str):
    image_url = ctx.message.attachments[0].url if ctx.message.attachments else None
    seconds = parse_duration(duration)
    desc = (
        "You must have any of these roles to particpate/enter in the server\n"
        "and here are their respective entries:\n\n"
        "<@&972809181444861984> **1x entries**\n<@&972809182224994354> **1x entries**\n"
        "<@&972809183374225478> **2x entries**\n<@&972809180966703176> **2x entries**\n"
        "<@&972809183718150144> **4x entries**\n<@&972809184242434048> **8x entries**\n"
        "<@&973502021757968414> **12x entries**\n\n"
        f"{E_ALERT} **Requirement:** Only users with these roles can enter.\n"
        f"{E_BOOST} **System:** Entries are calculated automatically."
    )
    donor_role_ids = list(DONOR_WEIGHTS.keys())
    await run_giveaway(ctx, prize, winners, seconds, desc, required_role_ids=donor_role_ids, weighted=True, image_url=image_url)

@bot.hybrid_command(name="giveaway_daily", description="Admin: Start Daily giveaway.")
@commands.has_permissions(administrator=True)
async def giveaway_daily(ctx, prize: str, winners: int, duration: str, *, description: str = "Daily Luck Test! React to enter."):
    image_url = ctx.message.attachments[0].url if ctx.message.attachments else None
    seconds = parse_duration(duration)
    await run_giveaway(ctx, prize, winners, seconds, description, required_role_ids=None, weighted=False, image_url=image_url)

@bot.hybrid_command(name="giveaway_shiny", description="Admin: Start Requirement giveaway.")
@commands.has_permissions(administrator=True)
async def giveaway_shiny(ctx, prize: str, winners: int, duration: str, required_role: discord.Role = None, *, description: str):
    image_url = ctx.message.attachments[0].url if ctx.message.attachments else None
    seconds = parse_duration(duration)
    full_desc = description
    role_id = None
    if required_role:
        role_id = required_role.id
        full_desc += f"\n\n{E_DANGER} **Requirement:** Must have {required_role.mention} to win."
    await run_giveaway(ctx, prize, winners, seconds, full_desc, required_role_ids=role_id, weighted=False, image_url=image_url)

# ===========================
#   GROUP 6: HELP & INFO
# ===========================

@bot.hybrid_command(name="botinfo", aliases=["info", "guide"], description="Open help panel.")
async def botinfo(ctx):
    embed = discord.Embed(
        title=f"{E_CROWN} **Ze Bot: Information & Guide**",
        description=(
            f"**Welcome to the ultimate Football Economy simulation!**\n\n"
            f"{E_STAR} **Why is this bot here?**\n"
            f"Built for this server to bring the thrill of the **Transfer Market** and **Club Ownership** to Discord. "
            f"Simulate the life of a wealthy investor or manager.\n\n"
            f"{E_FIRE} **Core Purpose**\n"
            f"• **Economy:** Earn money, trade assets, and build an empire.\n"
            f"• **Competition:** Buy real clubs, battle for Divisions, and prove who is the best.\n"
            f"• **Events:** Daily giveaways and auctions.\n\n"
            f"{E_ADMIN} **Credits**\n"
            f"Designed and created by **Soul Gill**."
        ),
        color=0xf1c40f
    )
    if bot.user.avatar: embed.set_thumbnail(url=bot.user.avatar.url)
    embed.add_field(name=f"{E_BOOK} **Navigation**", value="Click the buttons below to view commands.", inline=False)
    view = HelpView()
    await ctx.send(embed=embed, view=view)

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

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound): pass
    elif isinstance(error, commands.MissingRequiredArgument): await ctx.send(embed=create_embed("Error", f"Missing Argument: {error.param}", 0xff0000))
    elif isinstance(error, commands.MissingPermissions): await ctx.send(embed=create_embed("Error", "No Permission.", 0xff0000))
    elif isinstance(error, commands.BadArgument): await ctx.send(embed=create_embed("Error", str(error), 0xff0000))
    else: print(error)

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)
