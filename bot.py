# bot.py
# Full Club Auction Bot (Final Patched Build)
# Dependencies: discord.py, fastapi, uvicorn, jinja2, pymongo, dnspython, certifi

import os
import asyncio
import random
import re
from datetime import datetime, timedelta
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

# Image for Donor Giveaway
DONOR_THUMBNAIL_URL = "https://i.imgur.com/YourImageLinkHere.jpg" 

# Constants
TIME_LIMIT = 90 
MIN_INCREMENT_PERCENT = 5
LEAVE_PENALTY_PERCENT = 10
DUELIST_MISS_PENALTY_PERCENT = 15
WIN_VALUE_BONUS = 100000
LOSS_VALUE_PENALTY = -100000
OWNER_MSG_VALUE_BONUS = 10000
OWNER_MSG_COUNT_PER_BONUS = 100

# Donor Roles
DONOR_ROLES = {
    972809181444861984: 1, 972809182224994354: 1, 972809183374225478: 2,
    972809180966703176: 2, 972809183718150144: 4, 972809184242434048: 8,
    973502021757968414: 12
}

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

# ---------- DATABASE ----------
if not MONGO_URL:
    print("CRITICAL: MONGO_URL missing.")
    cluster = None; db = None
else:
    cluster = MongoClient(MONGO_URL, tlsCAFile=certifi.where())
    db = cluster["auction_bot"]

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
    giveaways_col = db.giveaways

def get_next_id(sequence_name):
    if db is None: return 0
    ret = counters_col.find_one_and_update({"_id": sequence_name}, {"$inc": {"seq": 1}}, upsert=True, return_document=ReturnDocument.AFTER)
    return ret['seq']

# ---------- BOT SETUP ----------
DEFAULT_PREFIX = "."
def get_prefix(bot, message):
    if db is None: return DEFAULT_PREFIX
    try:
        res = config_col.find_one({"key": "prefix"})
        return res["value"] if res else DEFAULT_PREFIX
    except: return DEFAULT_PREFIX

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix=get_prefix, intents=intents, help_command=None)
active_timers = {}
bidding_frozen = False

# ---------- UTILS ----------
class HumanAmount(commands.Converter):
    async def convert(self, ctx, argument):
        argument = argument.lower().replace(',', '').replace('$', '')
        multiplier = 1
        if argument.endswith('k'): multiplier = 1000; argument = argument[:-1]
        elif argument.endswith('m'): multiplier = 1000000; argument = argument[:-1]
        elif argument.endswith('b'): multiplier = 1000000000; argument = argument[:-1]
        try:
            val = float(argument) * multiplier
            return int(val)
        except ValueError: raise commands.BadArgument(f"Invalid amount: {argument}")

def create_embed(title, description, color=0x2ecc71, thumbnail=None, footer=None):
    embed = discord.Embed(title=title, description=description, color=color)
    if thumbnail: embed.set_thumbnail(url=thumbnail)
    if footer: embed.set_footer(text=footer)
    return embed

def resolve_emoji(item):
    if isinstance(item, int) or (isinstance(item, str) and item.isdigit()):
        e = bot.get_emoji(int(item))
        return str(e) if e else f"<:e:{item}>"
    if isinstance(item, str) and item.startswith(":") and item.endswith(":"):
        e = discord.utils.get(bot.emojis, name=item.strip(":"))
        return str(e) if e else item
    return str(item)

def log_audit(entry: str):
    if db is not None: audit_col.insert_one({"entry": entry, "timestamp": datetime.now()})

def log_user_activity(user_id, type, description):
    if db is not None: activities_col.insert_one({"user_id": str(user_id), "type": type, "description": description, "timestamp": datetime.now()})

async def send_log(channel_key, embed):
    if db is None: return
    cid = LOG_CHANNELS.get(channel_key)
    if cid:
        ch = bot.get_channel(cid)
        if ch: await ch.send(embed=embed)

# ---------- VIEWS ----------
class Paginator(View):
    def __init__(self, ctx, data, title, color, per_page=10):
        super().__init__(timeout=60)
        self.ctx, self.data, self.title, self.color, self.per_page = ctx, data, title, color, per_page
        self.current_page = 0
        self.total_pages = max(1, (len(data) + per_page - 1) // per_page)
        self.update_buttons()

    def update_buttons(self):
        self.children[0].disabled = self.current_page == 0
        self.children[1].disabled = self.current_page == self.total_pages - 1

    def get_embed(self):
        start = self.current_page * self.per_page
        page_data = self.data[start:start + self.per_page]
        embed = discord.Embed(title=f"{self.title} ({self.current_page + 1}/{self.total_pages})", color=self.color)
        if not page_data: embed.description = "No data available."
        for name, value in page_data: embed.add_field(name=name, value=value, inline=False)
        return embed

    @discord.ui.button(emoji="⬅️", style=discord.ButtonStyle.primary)
    async def prev_button(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return await interaction.response.send_message("Not for you.", ephemeral=True)
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(emoji="➡️", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return await interaction.response.send_message("Not for you.", ephemeral=True)
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

class BotInfoView(View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Economy", emoji=E_MONEY, style=discord.ButtonStyle.blurple)
    async def economy(self, interaction, button):
        desc = f"""
        **{E_MONEY} Economy Commands**
        `/profile` or `.p` - View stats, wallet, club.
        `/wallet` or `.bal` - Check balance.
        `/withdrawwallet` or `.ww` - Burn money from wallet.
        """
        await interaction.response.send_message(embed=create_embed("Economy", desc, 0x3498db), ephemeral=True)

    @discord.ui.button(label="Groups", emoji=E_PREMIUM, style=discord.ButtonStyle.blurple)
    async def groups(self, interaction, button):
        desc = f"""
        **{E_PREMIUM} Group Commands**
        `/creategroup` - Create an investment group.
        `/joingroup` - Join a group.
        `/groupinfo` or `.gi` - View group details.
        `/grouplist` - List all groups.
        `/deposit` or `.dep` - Pay to group bank.
        `/withdraw` or `.wd` - Withdraw from group bank.
        """
        await interaction.response.send_message(embed=create_embed("Groups", desc, 0x9b59b6), ephemeral=True)

    @discord.ui.button(label="Market", emoji=E_AUCTION, style=discord.ButtonStyle.blurple)
    async def market(self, interaction, button):
        desc = f"""
        **{E_AUCTION} Market Commands**
        `/placebid` or `.pb` - Bid on Club/Duelist (e.g. `.pb 1.5m`).
        `/groupbid` or `.gb` - Bid using Group funds.
        `/sellclub` or `.sc` - Sell your club.
        `/sellshares` or `.ss` - Sell group shares.
        `/listclubs` or `.lc` - View all clubs.
        `/marketlist` or `.ml` - View clubs for sale.
        `/marketbuy` or `.mb` - Buy unowned club.
        """
        await interaction.response.send_message(embed=create_embed("Market", desc, 0xe67e22), ephemeral=True)

    @discord.ui.button(label="Duelists", emoji=E_ITEMBOX, style=discord.ButtonStyle.blurple)
    async def duelists(self, interaction, button):
        desc = f"""
        **{E_ITEMBOX} Duelist Commands**
        `/registerduelist` or `.rd` - Join the draft.
        `/listduelists` or `.ld` - View all players.
        `/retireduelist` - Retire (needs owner approval).
        """
        await interaction.response.send_message(embed=create_embed("Duelists", desc, 0xf1c40f), ephemeral=True)
    
    @discord.ui.button(label="Updates", emoji=E_GIVEAWAY, style=discord.ButtonStyle.green)
    async def updates(self, interaction, button):
        desc = f"""
        **{E_GIVEAWAY} Recent Updates**
        • **Smart Syntax:** Use `1k`, `1.5m` in commands.
        • **Shortcuts:** `.pb` for placebid, `.ci` for clubinfo.
        • **Bot Info:** This new dashboard!
        • **Giveaways:** Donor, Daily, and Req giveaways added with file support.
        • **Payouts:** Admins can now payout and log automatically.
        • **Market:** Added `/marketlist` & `/marketbuy`.
        """
        await interaction.response.send_message(embed=create_embed("Changelog", desc, 0x2ecc71), ephemeral=True)

class GiveawayView(View):
    def __init__(self, giveaway_id=None, required_role_id=None):
        super().__init__(timeout=None) 
        self.giveaway_id, self.required_role_id = giveaway_id, required_role_id

    @discord.ui.button(label="React to Enter", emoji=E_GIVEAWAY, style=discord.ButtonStyle.success, custom_id="gw_join")
    async def join_button(self, interaction, button):
        if db is None: return
        gw = giveaways_col.find_one({"message_id": interaction.message.id})
        if not gw or gw.get("ended"): return await interaction.response.send_message("❌ Ended.", ephemeral=True)

        # Role Checks
        if gw.get("type") == "req":
            role = interaction.guild.get_role(int(gw["required_role_id"]))
            if role and role not in interaction.user.roles: return await interaction.response.send_message(f"Missing {role.mention}", ephemeral=True)
        
        entries = 1
        if gw.get("type") == "donor":
            user_roles = [r.id for r in interaction.user.roles]
            has_donor = False
            for rid, mul in DONOR_ROLES.items():
                if rid in user_roles: 
                    has_donor = True
                    if mul > entries: entries = mul
            if not has_donor: return await interaction.response.send_message("❌ Donor Only.", ephemeral=True)

        if giveaways_col.find_one({"message_id": interaction.message.id, "participants.user_id": interaction.user.id}):
            return await interaction.response.send_message("⚠️ Already joined.", ephemeral=True)
        
        giveaways_col.update_one({"message_id": interaction.message.id}, {"$push": {"participants": {"user_id": interaction.user.id, "entries": entries}}})
        await interaction.response.send_message(f"✅ Joined with **{entries} entries**!", ephemeral=True)

    @discord.ui.button(label="List", emoji="📋", style=discord.ButtonStyle.secondary, custom_id="gw_list")
    async def list_button(self, interaction, button):
        if not interaction.user.guild_permissions.administrator: return
        gw = giveaways_col.find_one({"message_id": interaction.message.id})
        parts = gw.get("participants", []) if gw else []
        txt = "\n".join([f"<@{p['user_id']}> ({p['entries']}x)" for p in parts[:20]])
        if len(parts) > 20: txt += f"\n...and {len(parts)-20} more."
        await interaction.response.send_message(f"**Participants:**\n{txt}" if txt else "None", ephemeral=True)

class MarketConfirmView(View):
    def __init__(self, ctx, club_id, price, buyer_id, is_group=False):
        super().__init__(timeout=60)
        self.ctx, self.club_id, self.price, self.buyer_id, self.is_group = ctx, club_id, price, buyer_id, is_group
        self.value = None

    @discord.ui.button(label="Confirm Buy", style=discord.ButtonStyle.success)
    async def confirm(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return
        
        # Check Funds again
        if self.is_group:
            g = groups_col.find_one({"name": self.buyer_id.replace("group:", "")})
            bal = g["funds"]
        else:
            w = wallets_col.find_one({"user_id": str(self.buyer_id)})
            bal = w["balance"] if w else 0
            
        if bal < self.price: return await interaction.response.send_message("Insufficient funds.", ephemeral=True)

        # Transaction
        if self.is_group: groups_col.update_one({"name": self.buyer_id.replace("group:", "")}, {"$inc": {"funds": -self.price}})
        else: wallets_col.update_one({"user_id": str(self.buyer_id)}, {"$inc": {"balance": -self.price}})

        clubs_col.update_one({"id": self.club_id}, {"$set": {"owner_id": str(self.buyer_id), "last_bid_price": self.price}})
        if not self.is_group:
            profiles_col.update_one({"user_id": str(self.buyer_id)}, {"$set": {"owned_club_id": self.club_id}}, upsert=True)
        
        log_user_activity(self.buyer_id, "Market", f"Bought Club ID {self.club_id} for ${self.price:,}")
        await interaction.response.send_message(embed=create_embed(f"{E_SUCCESS} Purchased!", f"Club ID {self.club_id} bought for ${self.price:,}", 0x2ecc71))
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return
        await interaction.response.send_message("Cancelled.", ephemeral=True)
        self.stop()

# ---------- CORE LOGIC ----------
def get_club_owner_info(club_id):
    if db is None: return None, []
    c = clubs_col.find_one({"id": int(club_id)})
    if not c or not c.get("owner_id"): return None, []
    owner = c["owner_id"]
    if owner.startswith('group:'):
        mems = group_members_col.find({"group_name": owner.replace('group:', '').lower()})
        return owner, [m['user_id'] for m in mems]
    return owner, [owner]

def min_required_bid(current):
    return int(current + max(1, round(current * MIN_INCREMENT_PERCENT / 100)))

def get_level_info(wins):
    lvl, nxt, req = LEVEL_UP_CONFIG[0][1], None, 0
    for w, n, b in LEVEL_UP_CONFIG:
        if w > wins: nxt, req = (n, w, b), w - wins; break
        elif w <= wins: lvl = n
    return lvl, nxt, req

def update_club_level(club_id, wins_gained=0):
    if db is None: return
    c = clubs_col.find_one({"id": int(club_id)})
    if not c: return
    new_wins = c.get("total_wins", 0) + wins_gained
    clubs_col.update_one({"id": int(club_id)}, {"$set": {"total_wins": new_wins}})
    for w, n, b in LEVEL_UP_CONFIG:
        if c.get("total_wins", 0) < w <= new_wins:
            clubs_col.update_one({"id": int(club_id)}, {"$set": {"level_name": n}, "$inc": {"value": b}})
            
async def finalize_auction(item_type, item_id, channel_id):
    if db is None: return
    bid = bids_col.find_one({"item_type": item_type, "item_id": int(item_id)}, sort=[("amount", -1)])
    ch = bot.get_channel(channel_id)
    item = clubs_col.find_one({"id": int(item_id)}) if item_type == "club" else duelists_col.find_one({"id": int(item_id)})
    
    if bid:
        bidder, amt = bid["bidder"], int(bid["amount"])
        if bidder.startswith('group:'): groups_col.update_one({"name": bidder.replace('group:', '').lower()}, {"$inc": {"funds": -amt}})
        else: wallets_col.update_one({"user_id": bidder}, {"$inc": {"balance": -amt}})
        
        if item_type == "club":
            old = item.get("owner_id")
            if old and not old.startswith("group:"): profiles_col.update_one({"user_id": old}, {"$unset": {"owned_club_id": ""}})
            history_col.insert_one({"club_id": int(item_id), "winner": bidder, "amount": amt, "timestamp": datetime.now()})
            clubs_col.update_one({"id": int(item_id)}, {"$set": {"owner_id": bidder, "value": amt, "ex_owner_id": old}})
            if not bidder.startswith('group:'): profiles_col.update_one({"user_id": bidder}, {"$set": {"owned_club_id": int(item_id)}}, upsert=True)
            
            embed = create_embed(f"{E_GIVEAWAY} SOLD!", f"{E_SUCCESS} **Owner:** {bidder}\n{E_MONEY} **Price:** ${amt:,}", 0xf1c40f, item.get('logo'))
            if ch: await ch.send(embed=embed)
            await send_log("club", embed)
        else:
            contracts_col.insert_one({"duelist_id": int(item_id), "club_owner": bidder, "amount": amt, "timestamp": datetime.now()})
            cid = None
            if bidder.startswith('group:'): 
                c = clubs_col.find_one({"owner_id": bidder})
                if c: cid = c['id']
            else: 
                c = clubs_col.find_one({"owner_id": bidder})
                if c: cid = c['id']
            duelists_col.update_one({"id": int(item_id)}, {"$set": {"owned_by": bidder, "club_id": cid}})
            wallets_col.update_one({"user_id": item["discord_user_id"]}, {"$inc": {"balance": amt}}, upsert=True)
            
            embed = create_embed(f"{E_GIVEAWAY} SIGNED!", f"{E_SUCCESS} **To:** {bidder}\n{E_MONEY} **Fee:** ${amt:,} (Paid to Player)", 0x9b59b6, item.get('avatar_url'))
            if ch: await ch.send(embed=embed)
            await send_log("duelist", embed)
    else:
        if ch: await ch.send(embed=create_embed(f"{E_TIMER} Ended", "No bids.", 0x95a5a6))
    
    bids_col.delete_many({"item_type": item_type, "item_id": int(item_id)})
    active_timers.pop((item_type, str(item_id)), None)

def schedule_auction_timer(item_type, item_id, channel_id):
    key = (item_type, str(item_id))
    if active_timers.get(key) and not active_timers.get(key).done(): active_timers[key].cancel()
    loop = asyncio.get_event_loop()
    t = loop.create_task(asyncio.sleep(TIME_LIMIT))
    async def wrapper():
        try: await t; await finalize_auction(item_type, item_id, channel_id)
        except asyncio.CancelledError: return
    active_timers[key] = loop.create_task(wrapper())

def parse_prize_amount(prize_str):
    prize_str = prize_str.lower().replace(',', '')
    mul = 1
    if 'k' in prize_str: mul = 1000; prize_str = prize_str.replace('k', '')
    elif 'm' in prize_str: mul = 1000000; prize_str = prize_str.replace('m', '')
    try: return int(float(prize_str) * mul)
    except: return None

async def end_giveaway(mid, ch, prize):
    if db is None: return
    gw = giveaways_col.find_one({"message_id": mid})
    if not gw or gw['ended']: return
    parts = gw.get('participants', [])
    if not parts:
        await ch.send(embed=create_embed("Ended", "No participants.", 0x95a5a6))
        return giveaways_col.update_one({"message_id": mid}, {"$set": {"ended": True}})
    
    pool = []
    for p in parts: pool.extend([p['user_id']] * p['entries'])
    winner_id = random.choice(pool)
    giveaways_col.update_one({"message_id": mid}, {"$set": {"ended": True, "winner_id": winner_id}})
    
    amt = parse_prize_amount(prize)
    msg = ""
    if amt:
        wallets_col.update_one({"user_id": str(winner_id)}, {"$inc": {"balance": amt}}, upsert=True)
        msg = f"\n{E_MONEY} **Auto-Tipped:** ${amt:,}"
    
    await ch.send(f"<@{winner_id}>", embed=create_embed(f"{E_GIVEAWAY} WINNER!", f"Congrats <@{winner_id}>!\nPrize: **{prize}**{msg}", 0xf1c40f))

# ---------- COMMANDS ----------

@bot.hybrid_command(name="botinfo", aliases=["info"], description="Show bot information and help.")
async def botinfo(ctx):
    embed = create_embed(f"{E_CROWN} Club Auction Bot", "The ultimate auction manager.", 0x3498db, bot.user.avatar.url if bot.user.avatar else None)
    embed.add_field(name="Stats", value=f"ping: {round(bot.latency*1000)}ms\nServers: {len(bot.guilds)}")
    view = BotInfoView()
    await ctx.send(embed=embed, view=view)

@bot.hybrid_command(name="payout", aliases=["po"], description="Admin: Pay user.")
@commands.has_permissions(administrator=True)
async def payout(ctx, user: discord.Member, amount: HumanAmount, *, reason: str):
    if amount <= 0: return
    w = wallets_col.find_one({"user_id": str(user.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send(embed=create_embed("Error", "Insufficient funds.", 0xff0000))
    wallets_col.update_one({"user_id": str(user.id)}, {"$inc": {"balance": -amount}})
    log_user_activity(user.id, "Payout", f"Cashed out ${amount:,}. Reason: {reason}")
    await send_log("withdraw", create_embed(f"{E_MONEY} Payout", f"**To:** {user.mention}\n**Amt:** ${amount:,}\n**Reason:** {reason}", 0xe74c3c))
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Paid", f"Paid ${amount:,} to {user.mention}", 0x2ecc71))

@bot.hybrid_command(name="logpayment", aliases=["lp"], description="Admin: Log manual payment.")
@commands.has_permissions(administrator=True)
async def logpayment(ctx, user: discord.Member, amount: HumanAmount, *, reason: str):
    await send_log("withdraw", create_embed(f"{E_ADMIN} Manual Log", f"**To:** {user.mention}\n**Amt:** ${amount:,}\n**Reason:** {reason}", 0x9b59b6))
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Logged", "Logged.", 0x2ecc71))

@bot.hybrid_command(name="gstart_donor", description="Start Donor Giveaway.")
@commands.has_permissions(administrator=True)
async def gstart_donor(ctx, time_str: str, winners: int, prize: str):
    seconds = 0
    if time_str.endswith("s"): seconds = int(time_str[:-1])
    elif time_str.endswith("m"): seconds = int(time_str[:-1]) * 60
    elif time_str.endswith("h"): seconds = int(time_str[:-1]) * 3600
    img = ctx.message.attachments[0].url if ctx.message.attachments else None
    end = datetime.now().timestamp() + seconds
    
    embed = create_embed(f"{E_GIVEAWAY} DONOR GIVEAWAY", f"**Prize:** {prize}\n**Ends:** <t:{int(end)}:R>\n\n**Multipliers Active!**", 0xff007f, img)
    view = GiveawayView(giveaway_id=ctx.message.id)
    msg = await ctx.send(embed=embed, view=view)
    giveaways_col.insert_one({"message_id": msg.id, "channel_id": ctx.channel.id, "type": "donor", "prize": prize, "winners": winners, "end_time": end, "participants": [], "ended": False})
    await asyncio.sleep(seconds)
    await end_giveaway(msg.id, ctx.channel, prize)

@bot.hybrid_command(name="gstart_daily", description="Start Daily Giveaway.")
@commands.has_permissions(administrator=True)
async def gstart_daily(ctx, time_str: str, winners: int, prize: str, *, description: str="Good Luck!"):
    seconds = 0
    if time_str.endswith("s"): seconds = int(time_str[:-1])
    elif time_str.endswith("m"): seconds = int(time_str[:-1]) * 60
    elif time_str.endswith("h"): seconds = int(time_str[:-1]) * 3600
    img = ctx.message.attachments[0].url if ctx.message.attachments else None
    end = datetime.now().timestamp() + seconds
    
    embed = create_embed(f"{E_GIVEAWAY} DAILY GIVEAWAY", f"**Prize:** {prize}\n**Ends:** <t:{int(end)}:R>\n\n{description}", 0x3498db, img)
    view = GiveawayView(giveaway_id=ctx.message.id)
    msg = await ctx.send(embed=embed, view=view)
    giveaways_col.insert_one({"message_id": msg.id, "channel_id": ctx.channel.id, "type": "daily", "prize": prize, "winners": winners, "end_time": end, "participants": [], "ended": False})
    await asyncio.sleep(seconds)
    await end_giveaway(msg.id, ctx.channel, prize)

@bot.hybrid_command(name="gstart_req", description="Start Req Giveaway.")
@commands.has_permissions(administrator=True)
async def gstart_req(ctx, time_str: str, winners: int, prize: str, role_id: int, *, description: str):
    seconds = 0
    if time_str.endswith("s"): seconds = int(time_str[:-1])
    elif time_str.endswith("m"): seconds = int(time_str[:-1]) * 60
    elif time_str.endswith("h"): seconds = int(time_str[:-1]) * 3600
    img = ctx.message.attachments[0].url if ctx.message.attachments else None
    end = datetime.now().timestamp() + seconds
    role = ctx.guild.get_role(role_id)
    
    embed = create_embed(f"{E_BOOST} REQ GIVEAWAY", f"**Prize:** {prize}\n**Req:** {role.mention if role else role_id}\n**Ends:** <t:{int(end)}:R>\n\n{description}", 0x9b59b6, img)
    view = GiveawayView(giveaway_id=ctx.message.id, required_role_id=role_id)
    msg = await ctx.send(embed=embed, view=view)
    giveaways_col.insert_one({"message_id": msg.id, "channel_id": ctx.channel.id, "type": "req", "prize": prize, "winners": winners, "required_role_id": role_id, "end_time": end, "participants": [], "ended": False})
    await asyncio.sleep(seconds)
    await end_giveaway(msg.id, ctx.channel, prize)

@bot.hybrid_command(name="placebid", aliases=["pb"], description="Place a bid.")
async def placebid(ctx, amount: HumanAmount, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return await ctx.send("Frozen.")
    item_type = item_type.lower()
    
    # Sold Check
    if item_type == "club":
        c = clubs_col.find_one({"id": item_id})
        is_active = (item_type, str(item_id)) in active_timers
        if c.get("owner_id") and not is_active: return await ctx.send(embed=create_embed(f"{E_ALERT} Sold Out", "Owned.", 0xff0000))
        prof = profiles_col.find_one({"user_id": str(ctx.author.id)})
        if prof and prof.get("owned_club_id"): return await ctx.send("You own a club.")

    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send("Poor.")
    
    req = min_required_bid(get_current_bid(item_type, item_id))
    if amount < req: return await ctx.send(f"Min: {req}")
    
    bids_col.insert_one({"bidder": str(ctx.author.id), "amount": amount, "item_type": item_type, "item_id": item_id, "timestamp": datetime.now()})
    log_user_activity(ctx.author.id, "Bid", f"{amount}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Bid", f"${amount:,}", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="groupbid", aliases=["gb"], description="Group bid.")
async def groupbid(ctx, group_name: str, amount: HumanAmount, item_type: str, item_id: int, club_name: str = None):
    if bidding_frozen: return
    g = groups_col.find_one({"name": group_name.lower()})
    if not g or g["funds"] < amount: return await ctx.send("Error")
    
    # Sold Check
    if item_type == "club":
         c = clubs_col.find_one({"id": int(item_id)})
         is_active = (item_type, str(item_id)) in active_timers
         if c.get("owner_id") and not is_active: return await ctx.send("Sold Out")
         if clubs_col.find_one({"owner_id": f"group:{group_name.lower()}"}): return await ctx.send("Group owns club")

    bids_col.insert_one({"bidder": f"group:{group_name.lower()}", "amount": amount, "item_type": item_type, "item_id": item_id})
    log_user_activity(ctx.author.id, "Bid", f"Group bid ${amount:,} on {item_type} {item_id}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Group Bid", f"${amount:,}", 0x2ecc71))
    schedule_auction_timer(item_type, item_id, ctx.channel.id)

@bot.hybrid_command(name="wallet", aliases=["bal"], description="Check balance.")
async def wallet(ctx):
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    bal = w["balance"] if w else 0
    await ctx.send(embed=create_embed(f"{E_MONEY} Wallet", f"${bal:,}", 0x2ecc71))

@bot.hybrid_command(name="deposit", aliases=["dep"], description="Deposit to group.")
async def deposit(ctx, group_name: str, amount: HumanAmount):
    if amount <= 0: return
    w = wallets_col.find_one({"user_id": str(ctx.author.id)})
    if not w or w.get("balance", 0) < amount: return await ctx.send("Poor")
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    groups_col.update_one({"name": group_name.lower()}, {"$inc": {"funds": amount}})
    log_user_activity(ctx.author.id, "Dep", f"${amount:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Deposited", f"${amount:,}", 0x2ecc71))

@bot.hybrid_command(name="withdraw", aliases=["wd"], description="Withdraw group.")
async def withdraw(ctx, group_name: str, amount: HumanAmount):
    g = groups_col.find_one({"name": group_name.lower()})
    if not g or g["funds"] < amount: return await ctx.send("Poor Group")
    groups_col.update_one({"name": group_name.lower()}, {"$inc": {"funds": -amount}})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": amount}})
    log_user_activity(ctx.author.id, "WD", f"${amount:,}")
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Withdrew", f"${amount:,}", 0x2ecc71))

@bot.hybrid_command(name="tip", aliases=["t"], description="Admin Tip.")
@commands.has_permissions(administrator=True)
async def tip(ctx, member: discord.Member, amount: HumanAmount):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": amount}}, upsert=True)
    log_user_activity(member.id, "Tip", f"${amount:,}")
    await ctx.send(embed=create_embed("Tipped", f"${amount:,}", 0x2ecc71))

@bot.hybrid_command(name="deduct_user", aliases=["du"], description="Admin Deduct.")
@commands.has_permissions(administrator=True)
async def deduct_user(ctx, member: discord.Member, amount: HumanAmount):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    log_user_activity(member.id, "Deduct", f"${amount:,}")
    await ctx.send(embed=create_embed("Deducted", f"${amount:,}", 0xe74c3c))

@bot.hybrid_command(name="adjustsalary", aliases=["adj"], description="Owner adjust.")
async def adjustsalary(ctx, duelist_id: int, amount: HumanAmount):
    d = duelists_col.find_one({"id": duelist_id})
    wallets_col.update_one({"user_id": str(ctx.author.id)}, {"$inc": {"balance": -amount}})
    wallets_col.update_one({"user_id": d["discord_user_id"]}, {"$inc": {"balance": amount}}, upsert=True)
    await ctx.send(embed=create_embed("Adjusted", f"${amount:,}", 0x2ecc71))

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

class MarketConfirmView(View):
    def __init__(self, ctx, club_id, price, buyer_id, is_group=False):
        super().__init__(timeout=60)
        self.ctx, self.club_id, self.price, self.buyer_id, self.is_group = ctx, club_id, price, buyer_id, is_group
        self.value = None

    @discord.ui.button(label="Confirm Buy", style=discord.ButtonStyle.success)
    async def confirm(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return
        
        # Check Funds again
        if self.is_group:
            g = groups_col.find_one({"name": self.buyer_id.replace("group:", "")})
            bal = g["funds"]
        else:
            w = wallets_col.find_one({"user_id": str(self.buyer_id)})
            bal = w["balance"] if w else 0
            
        if bal < self.price: return await interaction.response.send_message("Insufficient funds.", ephemeral=True)

        # Transaction
        if self.is_group: groups_col.update_one({"name": self.buyer_id.replace("group:", "")}, {"$inc": {"funds": -self.price}})
        else: wallets_col.update_one({"user_id": str(self.buyer_id)}, {"$inc": {"balance": -self.price}})

        clubs_col.update_one({"id": self.club_id}, {"$set": {"owner_id": str(self.buyer_id), "last_bid_price": self.price}})
        if not self.is_group:
            profiles_col.update_one({"user_id": str(self.buyer_id)}, {"$set": {"owned_club_id": self.club_id}}, upsert=True)

        await interaction.response.send_message(embed=create_embed(f"{E_SUCCESS} Purchased!", f"Club ID {self.club_id} bought for ${self.price:,}", 0x2ecc71))
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction, button):
        if interaction.user.id != self.ctx.author.id: return
        await interaction.response.send_message("Cancelled.", ephemeral=True)
        self.stop()

@bot.hybrid_command(name="marketbuy", aliases=["mb"], description="Buy an unowned club instantly.")
async def marketbuy(ctx, club_id: int, group_name: str = None):
    c = clubs_col.find_one({"id": club_id})
    if not c: return await ctx.send(embed=create_embed("Error", "Club not found.", 0xff0000))
    if c.get("owner_id"): return await ctx.send(embed=create_embed("Error", "Club is already owned.", 0xff0000))
    
    price = c["value"]
    buyer_id = str(ctx.author.id)
    is_group = False
    
    if group_name:
        g = groups_col.find_one({"name": group_name.lower()})
        if not g: return await ctx.send(embed=create_embed("Error", "Group not found.", 0xff0000))
        if not group_members_col.find_one({"group_name": group_name.lower(), "user_id": str(ctx.author.id)}): return await ctx.send("Not member.")
        if clubs_col.find_one({"owner_id": f"group:{group_name.lower()}"}): return await ctx.send("Group already owns a club.")
        if g["funds"] < price: return await ctx.send("Group poor.")
        buyer_id = f"group:{group_name.lower()}"
        is_group = True
    else:
        prof = profiles_col.find_one({"user_id": str(ctx.author.id)})
        if prof and prof.get("owned_club_id"): return await ctx.send("You own a club.")
        w = wallets_col.find_one({"user_id": str(ctx.author.id)})
        if not w or w.get("balance", 0) < price: return await ctx.send("Poor.")

    view = MarketConfirmView(ctx, club_id, price, buyer_id, is_group)
    await ctx.send(embed=create_embed(f"{E_AUCTION} Confirm Purchase", f"Buy **{c['name']}** for **${price:,}**?", 0xe67e22), view=view)

@bot.hybrid_command(name="marketlist", aliases=["ml"], description="View clubs for sale.")
async def marketlist(ctx):
    clubs = list(clubs_col.find({"owner_id": None}))
    data = []
    for c in clubs:
        data.append((f"{E_STAR} {c['name']} (ID: {c['id']})", f"{E_MONEY} Price: ${c['value']:,} | {E_BOOST} {c.get('level_name')}"))
    if not data: return await ctx.send(embed=create_embed("Market Empty", "No clubs available for sale.", 0x95a5a6))
    view = Paginator(ctx, data, f"{E_AUCTION} Market Listings", 0xe67e22, 10)
    await ctx.send(embed=view.get_embed(), view=view)

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
async def forcewinner(ctx, item_type: str, item_id: int, winner_str: str, amount: HumanAmount):
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
    
    win_trophy = resolve_emoji(E_WINNER_TROPHY)
    lose_mark = resolve_emoji(E_LOSER_MARK)
    
    final_banter = banter.format(
        winner=wc['name'], 
        loser=lc['name'], 
        w_emoji=winner_emoji, 
        l_emoji=loser_emoji
    )
    # Fallback
    final_banter = final_banter.replace("Team A", wc['name']).replace("Team B", lc['name'])

    embed_log = create_embed(
        f"{E_FIRE} Match Result", 
        f"{win_trophy} **Winner:** {wc['name']} {winner_emoji}\n"
        f"{lose_mark} **Loser:** {lc['name']} {loser_emoji}\n\n"
        f"_{final_banter}_", 
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
async def adjustgroupfunds(ctx, group_name: str, amount: HumanAmount):
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
async def tip(ctx, member: discord.Member, amount: HumanAmount):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": amount}}, upsert=True)
    log_user_activity(member.id, "Transaction", f"Received tip of ${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Tip", f"Added **${amount:,}** to {member.mention}.", 0xe67e22))

@bot.hybrid_command(name="deduct_user", description="Admin: Remove money from user.")
@commands.has_permissions(administrator=True)
async def deduct_user(ctx, member: discord.Member, amount: HumanAmount):
    wallets_col.update_one({"user_id": str(member.id)}, {"$inc": {"balance": -amount}}, upsert=True)
    log_user_activity(member.id, "Transaction", f"Deducted ${amount:,}")
    await ctx.send(embed=create_embed(f"{E_ADMIN} Admin Deduct", f"Removed **${amount:,}** from {member.mention}.", 0xff0000))

@bot.hybrid_command(name="setclubmanager", description="Admin: Set club manager.")
@commands.has_permissions(administrator=True)
async def setclubmanager(ctx, club_name: str, member: discord.Member):
    clubs_col.update_one({"name": {"$regex": f"^{club_name}$", "$options": "i"}}, {"$set": {"manager_id": str(member.id)}})
    await ctx.send(embed=create_embed(f"{E_SUCCESS} Manager Set", f"{member.mention} is now manager of {club_name}.", 0x2ecc71))

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} command(s)")
        bot.add_view(GiveawayView()) # Persistent Views
    except Exception as e: print(e)
    if not hasattr(bot, 'started'):
        bot.loop.create_task(market_simulation_task())
        bot.started = True

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound): pass
    elif isinstance(error, commands.MissingRequiredArgument): await ctx.send(embed=create_embed("Error", f"Missing Argument: {error.param}", 0xff0000))
    elif isinstance(error, commands.MissingPermissions): await ctx.send(embed=create_embed("Error", "No Permission.", 0xff0000))
    else: print(error)

if __name__ == "__main__":
    bot.run(DISCORD_TOKEN)