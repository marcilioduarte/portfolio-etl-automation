DO $$
DECLARE
    ano_inicio INTEGER := 2010;
    ano_fim INTEGER := 2019;
    nome_tabela TEXT;
    nome_particao TEXT;
BEGIN
    FOR ano IN ano_inicio..ano_fim LOOP
        nome_tabela := 'rais_vinculos_ano_' || ano;
        nome_particao := 'rais_vinculos_ano_' || ano;

        -- Criar a tabela de partição
        EXECUTE 'CREATE TABLE stg_rais.' || nome_tabela ||
                ' PARTITION OF stg_rais.tb_rais_vinculos FOR VALUES FROM (' || ano || ') TO (' || ano+1 || ');';

        -- Adicionar a restrição de verificação para o ano e a UF
        EXECUTE 'ALTER TABLE stg_rais.' || nome_tabela ||
                ' ADD CONSTRAINT dados_ano_' || ano || '_uf_check CHECK (uf in(''AC'', ''AL'', ''AM'', ''AP'', ''BA'', ''CE'', ''DF'', ''ES'', ''GO'', ''MA'', ''MG'', ''MS'', ''MT'', ''PA'', ''PB'', ''PE'', ''PI'', ''PR'', ''RJ'', ''RN'', ''RO'', ''RR'', ''RS'', ''SC'', ''SE'', ''SP'', ''TO'', ''NI''));';

        -- Criar um índice para a UF
        EXECUTE 'CREATE INDEX idx_dados_ano_uf_' || ano ||
                ' ON stg_rais.' || nome_tabela || ' (uf);';
    END LOOP;
END $$;