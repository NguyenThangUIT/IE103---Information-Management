create database quanlythoitiet123
Use QuanLyThoiTiet123
Go
CREATE TABLE KhuVuc (
    IDKhuVuc INT PRIMARY KEY,
    TenKhuVuc nvarchar(255),
    ViDo DECIMAL(9,6),
    KinhDo DECIMAL(9,6)
);

CREATE TABLE ThoiTiet (
    IDThoiTiet INT PRIMARY KEY,
    IDKhuVuc INT NOT NULL,
    NgayGio datetime ,
    NhietDo DECIMAL,
    DoAm DECIMAL,
    TocDoGio DECIMAL,
    HuongGio nvarchar(255),
    LuongMua DECIMAL,
    FOREIGN KEY (IDKhuVuc) REFERENCES KhuVuc(IDKhuVuc)
);

CREATE TABLE DuBao (
    IDDuBao INT PRIMARY KEY,
    IDKhuVuc INT NOT NULL,
    NgayGioDuBao datetime ,
    NhietDoDuBao DECIMAL,
    DoAmDuBao DECIMAL,
    TocDoGioDuBao DECIMAL,
    HuongGioDuBao nvarchar(255),
    LuongMuaDuBao DECIMAL,
    FOREIGN KEY (IDKhuVuc) REFERENCES KhuVuc(IDKhuVuc)
);

CREATE TABLE LoaiThoiTiet (
    IDLoaiThoiTiet INT PRIMARY KEY,
    TenLoaiThoiTiet nvarchar(255)
);

CREATE TABLE ThoiTiet_LoaiThoiTiet (
    IDThoiTiet INT NOT NULL,
    IDLoaiThoiTiet INT NOT NULL,
    PRIMARY KEY (IDThoiTiet, IDLoaiThoiTiet),
    FOREIGN KEY (IDThoiTiet) REFERENCES ThoiTiet(IDThoiTiet),
    FOREIGN KEY (IDLoaiThoiTiet) REFERENCES LoaiThoiTiet(IDLoaiThoiTiet)
);

CREATE TABLE NguoiDung (
    IDNguoiDung INT PRIMARY KEY,
    TenNguoiDung nvarchar(255),
    Email nvarchar(255),
    SoDienThoai nvarchar(15)
);
CREATE TABLE LogIn (
   IDNguoiDung INT PRIMARY KEY,
   Password nvarchar(255) ,
   FOREIGN KEY (IDNguoiDung) REFERENCES NguoiDung(IDNguoiDung)
);
CREATE TABLE ThietLap (
    IDNguoiDung INT PRIMARY KEY,
    NgonNgu nvarchar(255),
    CheDoXem nvarchar(255),
    FOREIGN KEY (IDNguoiDung) REFERENCES NguoiDung(IDNguoiDung)
);
CREATE TABLE QuanSat (
    IDQuanSat INT PRIMARY KEY,
    IDNguoiDung INT NOT NULL,
    IDThoiTiet INT NOT NULL,
    NgayGioQuanSat datetime ,
    GhiChu TEXT,
    FOREIGN KEY (IDNguoiDung) REFERENCES NguoiDung(IDNguoiDung),
    FOREIGN KEY (IDThoiTiet) REFERENCES ThoiTiet(IDThoiTiet)
);
CREATE TABLE ThongBao (
    IDThongBao INT PRIMARY KEY,
    IDKhuVuc INT NOT NULL,
    NgayGioThongBao datetime ,
    NoiDungThongBao TEXT,
    FOREIGN KEY (IDKhuVuc) REFERENCES KhuVuc(IDKhuVuc)
);
CREATE TABLE PhanHoiCongDong (
    IDPhanHoi INT PRIMARY KEY,
    IDNguoiDung INT NOT NULL,
    IDThoiTiet INT NOT NULL,
    NoiDungPhanHoi TEXT,
    ThoiGianPhanHoi datetime ,
    FOREIGN KEY (IDNguoiDung) REFERENCES NguoiDung(IDNguoiDung),
    FOREIGN KEY (IDThoiTiet) REFERENCES ThoiTiet(IDThoiTiet)
);
go
--trigger id nguoi dung phai khac nhau
create trigger idnguoidung
on nguoidung
for insert,update
as 
begin
	declare @id int
	select @id=IDNguoiDung from inserted
	if exists (select * from NguoiDung where @id=IDNguoiDung)
		begin
			print'id nguoi dung da ton tai, nhap lai'
			rollback transaction
		end
	else 
		begin
			print'them nguoi dung thanh cong';
		end
end
go
--Trigger kiểm tra khi thêm mới dữ liệu vào bảng ThoiTiet, nhiệt độ phải nằm trong khoảng hợp lý
CREATE TRIGGER CheckTemperature
ON ThoiTiet
FOR INSERT
AS
BEGIN
    IF EXISTS (SELECT * FROM inserted WHERE NhietDo < -20 OR NhietDo > 50)
    BEGIN
        RAISERROR ('Nhiệt độ phải nằm trong khoảng từ -20 đến 50 độ C.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
    ELSE BEGIN
        PRINT 'Thêm dữ liệu thành công'
    END
END;
go
--Trigger để kiểm tra khi thêm mới dữ liệu vào bảng DuBao, nhiệt độ dự báo cũng phải nằm trong khoảng hợp lý
CREATE TRIGGER CheckForecastTemperature
ON DuBao
FOR INSERT
AS
BEGIN
    IF EXISTS (SELECT * FROM inserted WHERE NhietDoDuBao < -20 OR NhietDoDuBao > 50)
    BEGIN
        RAISERROR ('Nhiệt độ dự báo phải nằm trong khoảng từ -20 đến 50 độ C.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
    ELSE BEGIN
        PRINT 'Thêm dữ liệu thành công'
    END
END;
go
--trigger cac khu vuc phai co kinh do vi do khac nhau 
create trigger khuvuckdvd
on khuvuc
for insert,update
as 
begin
	declare @kinhdo decimal(9,6),@vido decimal(9,6)
	select @kinhdo=KinhDo, @vido=ViDo from inserted
	if exists(select * from KhuVuc where @kinhdo=KinhDo and @vido=ViDo)
		begin
			print'vi tri nay da ton tai khu vuc khac'
			rollback transaction
		end
	else 
		begin
			print'thanh cong';
		end
End
go
--Trigger để kiểm tra khi xóa dữ liệu từ bảng KhuVuc, không cho phép xóa nếu khu vực đang có dữ liệu trong bảng ThoiTiet hoặc DuBao:
CREATE TRIGGER CheckAreaBeforeDelete
ON KhuVuc
FOR DELETE
AS
BEGIN
    IF EXISTS (SELECT * FROM deleted JOIN ThoiTiet ON deleted.IDKhuVuc = ThoiTiet.IDKhuVuc) OR EXISTS (SELECT * FROM deleted JOIN DuBao ON deleted.IDKhuVuc = DuBao.IDKhuVuc)
    BEGIN
        RAISERROR ('Không thể xóa khu vực này vì nó đang có dữ liệu trong bảng ThoiTiet hoặc DuBao.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
END;
go
--Trigger để kiểm tra khi xóa dữ liệu từ bảng ThietLap, không cho phép xóa nếu người dùng đang có dữ liệu trong bảng LogIn
CREATE TRIGGER CheckSettingsBeforeDelete
ON ThietLap
FOR DELETE
AS
BEGIN
    IF EXISTS (SELECT * FROM deleted JOIN LogIn ON deleted.IDNguoiDung = LogIn.IDNguoiDung)
    BEGIN
        RAISERROR ('Không thể xóa thiết lập này vì người dùng đang có dữ liệu trong bảng LogIn.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
END;
go
--Trigger để kiểm tra khi xóa dữ liệu từ bảng LoaiThoiTiet, không cho phép xóa nếu loại thời tiết đang có dữ liệu trong bảng ThoiTiet_LoaiThoiTiet:
CREATE TRIGGER CheckWeatherTypeBeforeDelete
ON LoaiThoiTiet
FOR DELETE
AS
BEGIN
    IF EXISTS (SELECT * FROM deleted JOIN ThoiTiet_LoaiThoiTiet ON deleted.IDLoaiThoiTiet = ThoiTiet_LoaiThoiTiet.IDLoaiThoiTiet)
    BEGIN
        RAISERROR ('Không thể xóa loại thời tiết này vì nó đang có dữ liệu trong bảng ThoiTiet_LoaiThoiTiet.', 16, 1);
        ROLLBACK TRANSACTION;
        RETURN;
    END
END;
go
--procedure dua vao id khu vuc, ngay thang xuat ra du bao thoi tiet khu vuc do trong ngay do
create procedure thoitietkhuvuc (
	@idkhuvuc int,
	@ngay datetime , 
	@dubao nvarchar(255) output
	)
as 
begin
			select @dubao = TenLoaiThoiTiet 
			from LoaiThoiTiet,ThoiTiet,ThoiTiet_LoaiThoiTiet
			where ThoiTiet.IDThoiTiet=ThoiTiet_LoaiThoiTiet.IDThoiTiet and ThoiTiet_LoaiThoiTiet.IDLoaiThoiTiet=LoaiThoiTiet.IDLoaiThoiTiet
			and @idkhuvuc=IDKhuVuc and NgayGio=@ngay
	if(@dubao is null)
	set @dubao='chua co du lieu'
	return @dubao;
End
go
--Procedure này nhận vào tên người dùng và trả về các phản hồi cộng đồng của họ.
CREATE PROCEDURE GetCommunityResponses
    @TenNguoiDung NVARCHAR(255)
AS
BEGIN
    IF EXISTS (SELECT 1 FROM PhanHoiCongDong JOIN NguoiDung ON PhanHoiCongDong.IDNguoiDung = NguoiDung.IDNguoiDung WHERE NguoiDung.TenNguoiDung = @TenNguoiDung)
    BEGIN
        SELECT PhanHoiCongDong.NoiDungPhanHoi, PhanHoiCongDong.ThoiGianPhanHoi
        FROM PhanHoiCongDong
        JOIN NguoiDung ON PhanHoiCongDong.IDNguoiDung = NguoiDung.IDNguoiDung
        WHERE NguoiDung.TenNguoiDung = @TenNguoiDung;
    END
    ELSE
    BEGIN
        SELECT 'Không có dữ liệu' AS ThongBao;
    END
END;
EXEC GetCommunityResponses @TenNguoiDung = N'Tên người dùng';
go
--Procedure để lấy thông tin chi tiết về các quan sát của một người dùng cụ thể:
CREATE PROCEDURE GetUserObservations
    @TenNguoiDung NVARCHAR(255)
AS
BEGIN
    BEGIN
        IF EXISTS (SELECT 1 FROM QuanSat JOIN NguoiDung ON QuanSat.IDNguoiDung = NguoiDung.IDNguoiDung WHERE NguoiDung.TenNguoiDung = @TenNguoiDung)
        BEGIN
            SELECT QuanSat.*
            FROM QuanSat
            JOIN NguoiDung ON QuanSat.IDNguoiDung = NguoiDung.IDNguoiDung
            WHERE NguoiDung.TenNguoiDung = @TenNguoiDung;
        END
        ELSE
        BEGIN
            SELECT 'Không có dữ liệu' AS ThongBao;
        END
    END
END;
GO
--function để tính trung bình nhiệt độ, độ ẩm của một khu vực từ dữ liệu của bảng THOITIET và DUBAO
CREATE FUNCTION GetAverageWeatherAndForecast(@TenKhuVuc NVARCHAR(255))
RETURNS @Result TABLE
(
    Source NVARCHAR(255),
    AvgTemperature DECIMAL(18, 2),
    AvgHumidity DECIMAL(18, 2)
)
AS
BEGIN
    DECLARE @IDKhuVuc INT;
    SELECT @IDKhuVuc = IDKhuVuc FROM KhuVuc WHERE TenKhuVuc = @TenKhuVuc;

    DECLARE @AvgTempThoiTiet DECIMAL(18, 2), @AvgHumidityThoiTiet DECIMAL(18, 2);
    DECLARE @AvgTempDuBao DECIMAL(18, 2), @AvgHumidityDuBao DECIMAL(18, 2);

    SELECT @AvgTempThoiTiet = AVG(NhietDo), @AvgHumidityThoiTiet = AVG(DoAm)
    FROM ThoiTiet
    WHERE IDKhuVuc = @IDKhuVuc;

    SELECT @AvgTempDuBao = AVG(NhietDoDuBao), @AvgHumidityDuBao = AVG(DoAmDuBao)
    FROM DuBao
    WHERE IDKhuVuc = @IDKhuVuc;

    INSERT INTO @Result
    VALUES ('Trung bình nhiệt độ, độ ẩm:', @AvgTempThoiTiet, @AvgHumidityThoiTiet),
           ('Trung bình nhiệt độ, độ ẩm dự báo:', @AvgTempDuBao, @AvgHumidityDuBao);

    RETURN;
END;
GO
SELECT * FROM GetAverageWeatherAndForecast(N'Tên khu vực');
GO
--function để xuất ra tên kiểu thời tiết phố biến nhất tại một khu vực
CREATE FUNCTION GetPopularWeatherType(@TenKhuVuc NVARCHAR(255))
RETURNS NVARCHAR(255)
AS
BEGIN
    DECLARE @IDKhuVuc INT, @TenLoaiThoiTiet NVARCHAR(255);

    SELECT @IDKhuVuc = IDKhuVuc FROM KhuVuc WHERE TenKhuVuc = @TenKhuVuc;

    SELECT TOP 1 @TenLoaiThoiTiet = LoaiThoiTiet.TenLoaiThoiTiet
    FROM ThoiTiet
    JOIN ThoiTiet_LoaiThoiTiet ON ThoiTiet.IDThoiTiet = ThoiTiet_LoaiThoiTiet.IDThoiTiet
    JOIN LoaiThoiTiet ON ThoiTiet_LoaiThoiTiet.IDLoaiThoiTiet = LoaiThoiTiet.IDLoaiThoiTiet
    WHERE ThoiTiet.IDKhuVuc = @IDKhuVuc
    GROUP BY LoaiThoiTiet.TenLoaiThoiTiet
    ORDER BY COUNT(*) DESC;

    RETURN CONCAT(@TenKhuVuc, ' hay ', @TenLoaiThoiTiet, '.');
END;
GO
SELECT dbo.GetPopularWeatherType(N'Tên khu vực') AS PopularWeatherType;
-- VIEW hiển thị thông tin về thời tiết hiện tại của khu vực
go
CREATE VIEW ThoiTiet_KhuVuc as
SELECT TT.IDThoiTiet, KV.IDKhuVuc, KV.TenKhuVuc, NgayGio, NhietDo, DoAm, TocDoGio, HuongGio, LuongMua, TenLoaiThoiTiet
FROM ThoiTiet as TT INNER JOIN KhuVuc as KV on TT.IDKhuVuc = KV.IDKhuVuc 
					INNER JOIN ThoiTiet_LoaiThoiTiet as TT_LTT on TT.IDThoiTiet = TT_LTT.IDThoiTiet
					INNER JOIN LoaiThoiTiet as LTT on TT_LTT.IDLoaiThoiTiet = LTT.IDLoaiThoiTiet
WHERE KV.TenKhuVuc = '...' and NgayGio = GETDATE()
-- Tạo VIEW hiển thị thông tin dự báo thời tiết của khu vực theo từng giờ
go
CREATE VIEW DuBao_ThoiTiet_Hour as
SELECT DB.IDDuBao, KV.IDKhuVuc, KV.TenKhuVuc, DATEPART(hour, NgayGioDuBao) as GioDuBao, NhietDoDuBao, DoAmDuBao, TocDoGioDuBao, HuongGioDuBao, LuongMuaDuBao, NoiDungThongBao
FROM DuBao as DB INNER JOIN KhuVuc as KV on DB.IDKhuVuc = KV.IDKhuVuc
				 INNER JOIN ThongBao as TB on KV.IDKhuVuc = TB.IDKhuVuc
WHERE KV.TenKhuVuc = '...' and NgayGioDuBao > GETDATE()	
-- Tạo VIEW hiển thị thông tin dự báo thời tiết của khu vực theo từng ngày
go
CREATE VIEW DuBao_ThoiTiet_Day as
SELECT DB.IDDuBao, KV.IDKhuVuc, KV.TenKhuVuc, DATEPART(day, NgayGioDuBao) as NgayDuBao, NhietDoDuBao, DoAmDuBao, TocDoGioDuBao, HuongGioDuBao, LuongMuaDuBao, NoiDungThongBao
FROM DuBao as DB INNER JOIN KhuVuc as KV on DB.IDKhuVuc = KV.IDKhuVuc
				 INNER JOIN ThongBao as TB on KV.IDKhuVuc = TB.IDKhuVuc
WHERE KV.TenKhuVuc = '...' and NgayGioDuBao > GETDATE()
GO
SELECT * FROM DuBao_ThoiTiet_Day
GO
-- Create role for admin
CREATE ROLE admin_role;
-- Create role for user
CREATE ROLE user_role;


-- Tạo các người dùng admin@; user@
CREATE LOGIN Login1 WITH PASSWORD = 'admin@123'
CREATE USER admin@ FOR LOGIN Login1

CREATE LOGIN Login2 WITH PASSWORD = 'user@123'
CREATE USER user@ FOR LOGIN Login2


-- Tạo nhóm: admin@ thuộc admin_role; user@ thuộc user_role
EXEC sp_addrolemember admin_role, admin@

EXEC sp_addrolemember user_role, user@

-- Phân quyền người dùng.

GRANT SELECT, INSERT, UPDATE, DELETE ON KhuVuc TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON ThoiTiet TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON DuBao TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON LoaiThoiTiet TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON ThoiTiet_LoaiThoiTiet TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON NguoiDung TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON ThietLap TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON QuanSat TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON ThongBao TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON PhanHoiCongDong TO admin_role

GRANT SELECT, INSERT, UPDATE, DELETE ON Login TO admin_role

GRANT INSERT, UPDATE ON QuanSat TO user_role

GRANT SELECT ON KhuVuc TO user_role

GRANT SELECT ON ThoiTiet TO user_role

GRANT SELECT ON LoaiThoiTiet TO user_role

GRANT SELECT ON ThoiTiet_LoaiThoiTiet TO user_role

GRANT SELECT ON NguoiDung TO user_role

GRANT SELECT ON ThietLap TO user_role

GRANT SELECT ON QuanSat TO user_role

GRANT SELECT ON ThongBao TO user_role

GRANT SELECT ON PhanHoiCongDong TO user_role

GRANT SELECT ON Login TO user_role
