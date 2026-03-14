import { supabase } from '../config/supabase.js';

export async function promoteToSilver(source: 'transactions' | 'rentals', since: string): Promise<void> {
  const rpc = source === 'transactions' ? 'promote_transactions' : 'promote_rentals';

  console.log(`[Promote] ${rpc}(since: ${since})`);
  const { error } = await supabase.rpc(rpc, { since });
  if (error) throw new Error(`Promote failed: ${error.message}`);
  console.log(`[Promote] ${rpc} complete`);
}

export async function refreshGold(source: 'transactions' | 'rentals', monthsBack: number = 3): Promise<void> {
  const rpc =
    source === 'transactions' ? 'refresh_market_data_from_transactions' : 'refresh_market_data_from_rentals';

  console.log(`[Gold] ${rpc}(months_back: ${monthsBack})`);
  const { error } = await supabase.rpc(rpc, { months_back: monthsBack });
  if (error) throw new Error(`Gold refresh failed: ${error.message}`);
  console.log(`[Gold] ${rpc} complete`);
}
