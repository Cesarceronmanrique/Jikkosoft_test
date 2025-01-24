DO $$
BEGIN
    CREATE TABLE IF NOT EXISTS CONSUMPTIONS.DIM_SECTORS (
        "SECTOR_ID" INT NOT NULL PRIMARY KEY,
        "SECTOR_NAME" VARCHAR(255) NOT NULL
    );
    IF NOT EXISTS (SELECT 1 FROM CONSUMPTIONS.DIM_SECTORS) THEN
        INSERT INTO CONSUMPTIONS.DIM_SECTORS ("SECTOR_ID", "SECTOR_NAME")
        VALUES
            (1, 'Comercial'),
            (2, 'Industrial'),
            (3, 'Oficial'),
            (4, 'Especial'),
            (5, 'Otros'),
            (6, 'Residencial');
    END IF;
END $$;
