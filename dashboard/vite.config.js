import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
export default defineConfig(({mode})=>({plugins:[react()], server:{proxy:{'/api':'http://127.0.0.1:18100'}},
 ...(mode==='hosted'?{define:{'import.meta.env.VITE_SNAPSHOT_URL':JSON.stringify('/published/snapshot.json'),'import.meta.env.VITE_PUBLISHED_DEMO':'true'}}:{})
}));
